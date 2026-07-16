"""
Shared execution/logging logic for the TPCH dbt Airflow DAG (no Cosmos).

Core idea per layer (bronze / silver / gold):
  1. Each layer has ONE fixed target-path: target/<layer>. Its last
     run_results.json is what every future trigger checks â€” whether that
     "future trigger" is a task Clear or a brand-new DAG trigger.
  2. Three possible states based on that file:
       NONE    -> never run since last reset -> `dbt build --select path:...`
       FAILED  -> has error/skipped node      -> `dbt retry` (reruns only those nodes)
       SUCCESS -> fully clean last time       -> SKIP entirely, no dbt call at all
     SKIP matters because Airflow itself has no concept of "an earlier, separate
     DagRun already finished this task, skip it here" â€” every new trigger
     re-executes every task in the graph. Without SKIP, re-triggering after a
     silver failure would also rebuild the already-successful bronze layer,
     which â€” given bronze's append incremental strategy â€” duplicates rows.
  3. THE CYCLE RESET (see reset_cycle() below, wired in as the DAG's final
     task after gold): once ALL FOUR layers succeed, reset_cycle wipes every
     layer's run_results.json. This is what makes "last run fully succeeded"
     mean "the next trigger reprocesses everything from bronze again" â€” which
     matters if you trigger the DAG multiple times a day. If any layer fails,
     reset_cycle never runs (its trigger rule is the Airflow default,
     all_success), so the failure state is left exactly as-is for the next
     trigger to resume from: succeeded layers SKIP, the failed layer RETRIES,
     downstream layers that never got a chance run for the first time.
  4. Either way (BUILD or RETRY), parse run_results.json afterwards and log one
     row per model to DBT_MODEL_LOG plus one summary row to DBT_RUN_LOG.
  5. If any model is still in error/skipped state after the attempt, raise
     AirflowException so the Airflow task is marked failed.
"""

import json
import os
import subprocess
import time
from datetime import datetime

import snowflake.connector
from cryptography.hazmat.primitives import serialization
from airflow.exceptions import AirflowException

# ---- adjust these for your environment ----
# These are CONTAINER paths (Airflow runs via Docker Compose) â€” see docker-compose.yaml:
#   /home/azureuser/airflow/dbt      -> /opt/airflow/dbt       (project + profiles.yml + keys)
#   /home/azureuser/airflow/dbt_venv -> /opt/airflow/dbt_venv  (separate venv where dbt is installed)
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"
DBT_BIN = "/opt/airflow/dbt/dbt_venv_clean/bin/dbt"  # isolated venv built INSIDE the container â€” avoids the
                                                        # protobuf<5 (Airflow) vs protobuf>=6 (dbt-core 1.12) conflict
                                                        # that comes from installing dbt into Airflow's shared env
CONTROL_DB = "CDCDATASTACK"
CONTROL_SCHEMA = "ETL_CONTROL"

# Same auth as profiles.yml (dev target) â€” reused directly so no separate
# Airflow connection needs to be created/maintained.
SNOWFLAKE_ACCOUNT = "pphvfju-fx43557"
SNOWFLAKE_USER = "ADF_SERVICE_USER"
SNOWFLAKE_ROLE = "ADF_ROLE"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_DATABASE = CONTROL_DB
SNOWFLAKE_PRIVATE_KEY_PATH = "/opt/airflow/dbt/keys/rsa_key.p8"
SNOWFLAKE_PRIVATE_KEY_PASSPHRASE = None  # profiles.yml has this blank

# Path-based selection â€” matches dbt_project.yml's folder-config layout
# (no +tags are set, so tag:bronze etc. would select nothing).
LAYER_SELECT = {
    "bronze": "path:models/tpch-dbt-snowflake/bronze",
    "silver": "path:models/tpch-dbt-snowflake/silver",
    "snapshot": "path:snapshots/tpch",   # dim_customer / dim_supplier SCD2 source
    "gold": "path:models/tpch-dbt-snowflake/gold",
}
# --------------------------------------------


def _load_private_key_der() -> bytes:
    with open(SNOWFLAKE_PRIVATE_KEY_PATH, "rb") as f:
        p_key = serialization.load_pem_private_key(
            f.read(),
            password=SNOWFLAKE_PRIVATE_KEY_PASSPHRASE.encode() if SNOWFLAKE_PRIVATE_KEY_PASSPHRASE else None,
        )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class SnowflakeControlConn:
    """Direct snowflake-connector-python session using the same key-pair auth
    as dbt's profiles.yml. Replaces SnowflakeHook so no Airflow connection
    object needs to be created/maintained separately."""

    def __enter__(self):
        self._conn = snowflake.connector.connect(
            account=SNOWFLAKE_ACCOUNT,
            user=SNOWFLAKE_USER,
            role=SNOWFLAKE_ROLE,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            private_key=_load_private_key_der(),
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def run(self, sql: str):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
        finally:
            cur.close()

    def get_first(self, sql: str):
        cur = self._conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchone()
        finally:
            cur.close()


def _target_path_for(layer: str) -> str:
    return os.path.join("target", layer)


def _run_results_file(layer: str) -> str:
    return os.path.join(DBT_PROJECT_DIR, _target_path_for(layer), "run_results.json")


def _last_attempt_status(results_file: str) -> str:
    """Returns 'NONE' (never run), 'FAILED' (unresolved error/skip), or 'SUCCESS'."""
    if not os.path.exists(results_file):
        return "NONE"
    with open(results_file) as f:
        run_results = json.load(f)
    node_results = run_results.get("results", [])
    if any(r.get("status") in ("error", "fail", "skipped") for r in node_results):
        return "FAILED"
    return "SUCCESS"


def _run_subprocess(cmd: list) -> tuple:
    start = time.time()
    proc = subprocess.run(
        cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True
    )
    duration = time.time() - start
    print(proc.stdout)
    if proc.returncode != 0:
        print(proc.stderr)
    return proc.returncode, duration


def _insert_model_rows(hook: SnowflakeControlConn, run_log_id: int, dag_id: str, dag_run_id: str,
                        layer: str, attempt_type: str, results: list):
    if not results:
        return
    rows = []
    for r in results:
        unique_id = r.get("unique_id", "")
        resource_type = unique_id.split(".")[0] if unique_id else "unknown"
        node_name = unique_id.split(".")[-1]
        status = r.get("status")
        exec_time = r.get("execution_time", 0)
        error_msg = None
        if status in ("error", "fail"):
            error_msg = (r.get("message") or "")[:4000].replace("'", "''")
        rows_affected = None
        adapter_resp = r.get("adapter_response") or {}
        if "rows_affected" in adapter_resp:
            rows_affected = adapter_resp["rows_affected"]

        rows.append(
            f"""({run_log_id}, '{dag_id}', '{dag_run_id}', '{layer}', '{node_name}',
                 '{resource_type}', '{status}', {exec_time}, {rows_affected if rows_affected is not None else 'NULL'},
                 {("'" + error_msg + "'") if error_msg else 'NULL'}, '{attempt_type}')"""
        )

    values_sql = ",\n".join(rows)
    hook.run(f"""
        INSERT INTO {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_MODEL_LOG
        (RUN_LOG_ID, DAG_ID, DAG_RUN_ID, LAYER, MODEL_NAME, RESOURCE_TYPE, STATUS,
         EXECUTION_TIME_SECONDS, ROWS_AFFECTED, ERROR_MESSAGE, ATTEMPT_TYPE)
        VALUES
        {values_sql}
    """)


def _insert_run_summary(hook: SnowflakeControlConn, dag_id: str, dag_run_id: str, logical_date,
                         layer: str, attempt_type: str, dbt_command: str, status: str,
                         totals: dict, start_time: datetime, end_time: datetime) -> int:
    duration = (end_time - start_time).total_seconds()
    hook.run(f"""
        INSERT INTO {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_RUN_LOG
        (DAG_ID, DAG_RUN_ID, LOGICAL_DATE, LAYER, ATTEMPT_TYPE, DBT_COMMAND, STATUS,
         MODELS_TOTAL, MODELS_SUCCESS, MODELS_ERROR, MODELS_SKIPPED,
         START_TIME, END_TIME, DURATION_SECONDS)
        VALUES
        ('{dag_id}', '{dag_run_id}', '{logical_date}', '{layer}', '{attempt_type}',
         '{dbt_command}', '{status}',
         {totals['total']}, {totals['success']}, {totals['error']}, {totals['skipped']},
         '{start_time}', '{end_time}', {duration})
    """)
    result = hook.get_first(f"""
        SELECT RUN_LOG_ID FROM {CONTROL_DB}.{CONTROL_SCHEMA}.DBT_RUN_LOG
        WHERE DAG_ID = '{dag_id}' AND DAG_RUN_ID = '{dag_run_id}' AND LAYER = '{layer}'
        ORDER BY RUN_LOG_ID DESC LIMIT 1
    """)
    return result[0]


def reset_cycle(**context):
    """Runs only after gold succeeds (default Airflow trigger rule = all_success
    means this task doesn't even start if any upstream layer failed). Wipes
    every layer's run_results.json so the NEXT trigger sees NONE for all four
    layers and does a full fresh build â€” this is what makes 'last run fully
    succeeded -> next trigger reprocesses everything' work, while a partial
    failure (this task never runs) leaves state in place for the next trigger
    to resume from exactly where it broke."""
    import shutil
    for layer in LAYER_SELECT:
        layer_dir = os.path.join(DBT_PROJECT_DIR, "target", layer)
        if os.path.exists(layer_dir):
            shutil.rmtree(layer_dir)
            print(f"[reset_cycle] cleared {layer_dir}")


def execute_layer(layer: str, **context):
    dag_id = context["dag"].dag_id
    dag_run_id = context["dag_run"].run_id
    logical_date = context["logical_date"]

    results_file = _run_results_file(layer)
    last_status = _last_attempt_status(results_file)
    target_path = _target_path_for(layer)

    if last_status == "SUCCESS":
        # Already completed cleanly on a previous attempt. Airflow itself has no
        # concept of "skip this task, an earlier DagRun already finished it" â€” every
        # new trigger re-executes every task. Without this check, a fresh trigger
        # would call `dbt build` again here, which (given bronze's append strategy)
        # would duplicate rows. To force a genuine full rebuild of this layer,
        # manually delete: <results_file>
        print(f"[{layer}] already succeeded previously â€” skipping (target-path: {target_path}). "
              f"Delete {results_file} to force a full rebuild.")
        with SnowflakeControlConn() as hook:
            _insert_run_summary(
                hook, dag_id, dag_run_id, logical_date, layer, "SKIP",
                "(skipped â€” previous attempt already succeeded)", "SKIPPED",
                {"total": 0, "success": 0, "error": 0, "skipped": 0},
                datetime.utcnow(), datetime.utcnow(),
            )
        return

    if last_status == "FAILED":
        attempt_type = "RETRY"
        cmd = [
            DBT_BIN, "retry",
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROFILES_DIR,
            "--target-path", target_path,
        ]
    else:  # NONE â€” never attempted
        attempt_type = "BUILD"
        if layer not in LAYER_SELECT:
            raise AirflowException(f"Unknown layer '{layer}' â€” add it to LAYER_SELECT.")
        cmd = [
            DBT_BIN, "build",
            "--select", LAYER_SELECT[layer],
            "--project-dir", DBT_PROJECT_DIR,
            "--profiles-dir", DBT_PROFILES_DIR,
            "--target-path", target_path,
        ]

    print(f"[{layer}] attempt_type={attempt_type} cmd={' '.join(cmd)}")

    start_time = datetime.utcnow()
    returncode, _ = _run_subprocess(cmd)
    end_time = datetime.utcnow()

    if not os.path.exists(results_file):
        raise AirflowException(
            f"[{layer}] dbt produced no run_results.json at {results_file} â€” "
            f"check dbt/profile config. returncode={returncode}"
        )

    with open(results_file) as f:
        run_results = json.load(f)

    node_results = run_results.get("results", [])
    totals = {
        "total": len(node_results),
        "success": sum(1 for r in node_results if r.get("status") == "success"),
        "error": sum(1 for r in node_results if r.get("status") in ("error", "fail")),
        "skipped": sum(1 for r in node_results if r.get("status") == "skipped"),
    }
    overall_status = "SUCCESS" if totals["error"] == 0 and totals["skipped"] == 0 else "FAILED"

    with SnowflakeControlConn() as hook:
        run_log_id = _insert_run_summary(
            hook, dag_id, dag_run_id, logical_date, layer, attempt_type,
            " ".join(cmd), overall_status, totals, start_time, end_time,
        )
        _insert_model_rows(hook, run_log_id, dag_id, dag_run_id, layer, attempt_type, node_results)

    print(f"[{layer}] {attempt_type} totals: {totals}")

    if overall_status == "FAILED":
        failed_models = [
            r.get("unique_id", "").split(".")[-1]
            for r in node_results if r.get("status") in ("error", "fail", "skipped")
        ]
        raise AirflowException(
            f"[{layer}] {totals['error']} error, {totals['skipped']} skipped: {failed_models}. "
            f"Clear this task to retry â€” only these models will be rerun."
        )
