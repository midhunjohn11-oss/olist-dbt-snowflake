"""
Per-model logging + restart-state callbacks for the Cosmos DAG.

Unlike the plain DAG (which parses dbt's run_results.json after a whole-layer
`dbt build`), Cosmos gives each dbt model its own Airflow task. So here, the
Airflow TASK's own success/failure IS the model's result â€” no run_results.json
parsing needed. (Confirmed empirically: Cosmos's per-task run_results.json
lives in an ephemeral temp dir that's cleaned up immediately after the task
finishes â€” not readable after the fact, unlike the plain DAG's fixed path.)

TWO separate Snowflake tables are involved:
  - DBT_MODEL_LOG: audit log, INSERT-ONLY, one row per attempt, kept forever.
  - DBT_MODEL_CYCLE_STATE: current-cycle state, one row per (DAG_ID, MODEL_NAME),
    UPSERTed on every task completion, WIPED by reset_cycle on full pipeline
    success. This is what makes zero-intervention restart possible: before a
    task runs, its pre_execute hook (see tpch_dbt_pipeline_cosmos.py) checks
    this table â€” if the model already succeeded THIS cycle, the task is
    skipped entirely; otherwise it runs (which naturally means "retry" for a
    previously-failed model, since Cosmos tasks are already per-model).

ROWS_AFFECTED: since dbt's own run_results.json isn't readable after the task
completes, row counts are looked up from Snowflake's own query history
instead (INFORMATION_SCHEMA.QUERY_HISTORY â€” near-real-time, no ACCOUNT_USAGE
replication lag), matching write queries within the task's time window whose
QUERY_TEXT mentions the model name. This is a BEST-EFFORT match, not a
guaranteed-exact figure the way the plain DAG's run_results.json parsing is â€”
it can occasionally over/under-count if multiple queries referencing the same
model name run in the same narrow window.

Layer is inferred from model naming convention â€” adjust LAYER_PREFIXES if
actual model names don't match:
  stg_tpch__*      -> bronze
  tpch_silver_*    -> silver
  snap_tpch_*      -> snapshot
  tpch_dim_/fact_/mart_/date -> gold
"""

from datetime import timedelta

from utils.dbt_airflow_utils import SnowflakeControlConn, CONTROL_DB, CONTROL_SCHEMA

LAYER_PREFIXES = [
    ("stg_tpch__", "bronze"),
    ("snap_tpch_", "snapshot"),
    ("tpch_silver_", "silver"),
    ("tpch_dim_", "gold"),
    ("tpch_fact_", "gold"),
    ("tpch_mart_", "gold"),
    ("tpch_date", "gold"),
]


def _infer_layer(model_name: str) -> str:
    for prefix, layer in LAYER_PREFIXES:
        if model_name.startswith(prefix):
            return layer
    return "unknown"


def _base_model_name(task_id: str) -> str:
    # Cosmos task_ids are typically the model name, sometimes suffixed with
    # the dbt sub-command (e.g. "<model>_run", "<model>_test") or dotted
    # inside a per-model TaskGroup ("<model>.run"). Strip either pattern.
    name = task_id.split(".")[0]
    for suffix in ("_run", "_test", "_snapshot"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    return name


def get_cycle_status(dag_id: str, model_name: str) -> str:
    """Returns 'SUCCESS', 'FAILED', or 'NONE' (never run this cycle)."""
    with SnowflakeControlConn() as hook:
        row = hook.get_first(f"""
            SELECT STATUS FROM {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_MODEL_CYCLE_STATE
            WHERE DAG_ID = '{dag_id}' AND MODEL_NAME = '{model_name}'
        """)
    if row is None:
        return "NONE"
    return row[0]


def _upsert_cycle_state(hook, dag_id: str, model_name: str, layer: str, status: str):
    hook.run(f"""
        MERGE INTO {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_MODEL_CYCLE_STATE AS tgt
        USING (SELECT '{dag_id}' AS DAG_ID, '{model_name}' AS MODEL_NAME,
                      '{layer}' AS LAYER, '{status}' AS STATUS) AS src
        ON tgt.DAG_ID = src.DAG_ID AND tgt.MODEL_NAME = src.MODEL_NAME
        WHEN MATCHED THEN UPDATE SET STATUS = src.STATUS, LAYER = src.LAYER,
                                      UPDATED_AT = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT (DAG_ID, MODEL_NAME, LAYER, STATUS)
                               VALUES (src.DAG_ID, src.MODEL_NAME, src.LAYER, src.STATUS)
    """)


def reset_cosmos_cycle(**context):
    """Wired in as the DAG's final task (trigger_rule=NONE_FAILED, downstream
    of every leaf task). Wipes this DAG's cycle state so the NEXT trigger
    reprocesses everything fresh â€” mirrors reset_cycle in the plain DAG."""
    dag_id = context["dag"].dag_id
    with SnowflakeControlConn() as hook:
        hook.run(f"""
            DELETE FROM {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_MODEL_CYCLE_STATE
            WHERE DAG_ID = '{dag_id}'
        """)
    print(f"[reset_cycle] cleared cycle state for {dag_id}")


def _fetch_rows_affected(hook, model_name: str, start_date, end_date):
    """Best-effort lookup via Snowflake's own query history â€” Cosmos's per-task
    run_results.json isn't readable after the fact (ephemeral temp dir), so
    this is the fallback. Widens the window slightly for clock skew. Returns
    None (not 0) if nothing matched, so NULL is stored rather than a false 0."""
    if not start_date or not end_date:
        return None
    start_str = (start_date - timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")
    end_str = (end_date + timedelta(seconds=10)).strftime("%Y-%m-%d %H:%M:%S")
    safe_model_name = model_name.replace("'", "''")
    try:
        row = hook.get_first(f"""
            SELECT SUM(COALESCE(ROWS_INSERTED, 0) + COALESCE(ROWS_UPDATED, 0)) AS rows_affected
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => '{start_str}'::timestamp_ltz,
                END_TIME_RANGE_END => '{end_str}'::timestamp_ltz
            ))
            WHERE QUERY_TEXT ILIKE '%{safe_model_name}%'
              AND EXECUTION_STATUS = 'SUCCESS'
              AND QUERY_TYPE IN ('INSERT', 'MERGE', 'CREATE_TABLE_AS_SELECT', 'UPDATE')
        """)
    except Exception as e:
        print(f"[rows_affected lookup failed for {model_name}]: {e}")
        return None
    return row[0] if row and row[0] is not None else None


def _log_model_event(context, status: str):
    ti = context["task_instance"]
    dag_id = context["dag"].dag_id
    dag_run_id = context["dag_run"].run_id
    model_name = _base_model_name(ti.task_id)
    layer = _infer_layer(model_name)

    exec_time = None
    if ti.start_date and ti.end_date:
        exec_time = (ti.end_date - ti.start_date).total_seconds()

    error_message = None
    if status == "error":
        exc = context.get("exception")
        if exc:
            error_message = str(exc)[:4000].replace("'", "''")

    with SnowflakeControlConn() as hook:
        rows_affected = None
        if status == "success":
            rows_affected = _fetch_rows_affected(hook, model_name, ti.start_date, ti.end_date)

        hook.run(f"""
            INSERT INTO {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_MODEL_LOG
            (RUN_LOG_ID, DAG_ID, DAG_RUN_ID, LAYER, MODEL_NAME, RESOURCE_TYPE, STATUS,
             EXECUTION_TIME_SECONDS, ROWS_AFFECTED, ERROR_MESSAGE, ATTEMPT_TYPE)
            VALUES
            (NULL, '{dag_id}', '{dag_run_id}', '{layer}', '{model_name}',
             'model', '{status}', {exec_time if exec_time is not None else 'NULL'},
             {rows_affected if rows_affected is not None else 'NULL'},
             {("'" + error_message + "'") if error_message else 'NULL'}, 'COSMOS')
        """)
        cycle_status = "SUCCESS" if status == "success" else "FAILED"
        _upsert_cycle_state(hook, dag_id, model_name, layer, cycle_status)


def log_model_success(context):
    _log_model_event(context, "success")


def log_model_failure(context):
    _log_model_event(context, "error")
