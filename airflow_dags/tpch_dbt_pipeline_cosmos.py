"""
tpch_dbt_pipeline_cosmos
--------------------------
Cosmos-generated DAG: one Airflow task per dbt model/snapshot, instead of one
task per layer like tpch_dbt_pipeline_plain. Compare the two directly.

RESTARTABILITY â€” zero-intervention, matches the plain DAG's behavior:
Because each model is its own Airflow task, clearing a single failed task
reruns ONLY that model and its downstream chain â€” that part is native to
Cosmos, no extra code needed.

What ISN'T native: Airflow has no concept of "an earlier, separate DagRun
already finished this task, skip it here" â€” every fresh `airflow dags
trigger` re-executes every task from scratch, including already-successful
ones. Left alone, that would re-run bronze's `incremental_strategy: append`
models and duplicate rows, exactly like it did on the plain DAG before we
added reset_cycle there.

Fixed here the same way, adapted to Cosmos's per-model task shape (see
cosmos_logging_callbacks.py for the actual implementation):
  - A CYCLE-STATE table (DBT_MODEL_CYCLE_STATE, separate from the
    insert-only audit log DBT_MODEL_LOG) tracks one row per model: did it
    succeed or fail THIS cycle.
  - Every generated task gets a `pre_execute` hook (patched in below, after
    DbtDag builds the graph) that checks this table BEFORE the task runs:
    already SUCCESS this cycle -> raise AirflowSkipException, skip it
    entirely; otherwise -> let it run (which for a previously-FAILED model
    is effectively "retry", since Cosmos tasks are already single-model).
  - A `reset_cycle` task is appended after every leaf task, wired with
    trigger_rule=NONE_FAILED. It wipes DBT_MODEL_CYCLE_STATE once the whole
    pipeline is clean, so the NEXT trigger reprocesses everything from
    bronze again â€” exactly the plain DAG's "full success means next run
    starts fresh" behavior.
  - IMPORTANT CORRECTNESS DETAIL: every task's trigger_rule is set to
    NONE_FAILED (not the Airflow default ALL_SUCCESS). A skipped upstream
    task reports state SKIPPED, not SUCCESS â€” under the default rule that
    would incorrectly cascade-skip everything downstream of it too, even a
    task that genuinely needs to run. NONE_FAILED means "proceed as long as
    nothing upstream actually failed," which is what we want.

This monkeypatches pre_execute at the AIRFLOW level (a stable, public
BaseOperator hook), not Cosmos's internal operator classes â€” chosen
deliberately to be less exposed to Cosmos API drift than subclassing would
be, given how much version-specific breakage we already hit just getting
this DAG to import (RenderConfig kwargs, operator_args validation, etc.).

LOGGING â€” reuses the SAME Snowflake control tables as the plain DAG
(DBT_RUN_LOG / DBT_MODEL_LOG under CDCDATASTACK.ETL_CONTROL), via
on_success_callback / on_failure_callback attached to every generated task.
Since each Airflow task IS one model here, logging reads status/timing
straight off the TaskInstance â€” no run_results.json parsing needed.
ATTEMPT_TYPE is always logged as 'COSMOS'.

REQUIRES:
  - `astronomer-cosmos` installed in Airflow's shared environment (needed for
    `from cosmos import DbtDag` at DAG-parse time) via _PIP_ADDITIONAL_REQUIREMENTS.
  - dbt itself is DELIBERATELY NOT in that shared environment â€” dbt-core 1.12
    requires protobuf>=6, Airflow's own OpenTelemetry stack requires
    protobuf<5. Installing both in the same env silently corrupts one or the
    other. dbt lives in an isolated venv instead, built INSIDE the container
    (not copied from the host â€” that has its own shebang/binary-compatibility
    problems, also learned the hard way): /opt/airflow/dbt/dbt_venv_clean.
    Cosmos's default LOCAL execution mode + DBT_LS load mode shell out to
    dbt_executable_path rather than importing dbt-core as a library, so this
    works without needing dbt-core in Airflow's own environment.
  - Same profiles.yml / key-pair auth as the plain DAG â€” reused directly,
    no new Snowflake connection needed.
  - dbt_packages/ already populated (dbt deps was already run once for the
    plain DAG; dbt_deps=False below skips re-running it here â€” this is a
    project-level artifact, independent of which dbt executable runs it).
  - CDCDATASTACK.ETL_CONTROL.DBT_MODEL_CYCLE_STATE table created (see
    sql/create_cycle_state_table.sql) â€” one-time DDL, separate from the
    plain DAG's control tables.

NOTE ON VERSION DRIFT: Cosmos's exact API (ProjectConfig/RenderConfig fields,
generated task_id naming) has shifted across releases. Treat this as a first
pass to debug against your actual installed version the same way we debugged
the plain DAG â€” task_id naming in particular (see _base_model_name in the
callbacks module) may need adjusting once you see real task_ids in the UI.
"""

from datetime import datetime
import types

from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig

from utils.cosmos_logging_callbacks import (
    _base_model_name,
    get_cycle_status,
    log_model_failure,
    log_model_success,
    reset_cosmos_cycle,
)

DBT_PROJECT_DIR = "/opt/airflow/dbt"

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_DIR,
    # Setting manifest_path is enough on its own to trigger manifest-based
    # loading in this Cosmos version â€” RenderConfig has no load_mode kwarg
    # here (that's from a different Cosmos release; adjust if yours differs).
    manifest_path=f"{DBT_PROJECT_DIR}/target/manifest.json",
)

profile_config = ProfileConfig(
    profile_name="olist_dbt_project",
    target_name="dev",
    profiles_yml_filepath=f"{DBT_PROJECT_DIR}/profiles.yml",
)

render_config = RenderConfig(
    # Scope to the TPCH models + its snapshots only â€” the project also
    # contains the separate olist_dbt_project bronze/silver/gold models,
    # which this DAG should NOT touch.
    select=["path:models/tpch-dbt-snowflake", "path:snapshots/tpch"],
    dbt_deps=False,  # dbt_packages/ already installed once, persists on the mounted volume
    # IMPORTANT: manifest.json (see ProjectConfig above) must be regenerated
    # (`dbt parse`) any time TPCH models/snapshots change, or this DAG's
    # graph will go stale silently.
)

execution_config = ExecutionConfig(
    # Isolated venv built INSIDE the container â€” same one the plain DAG uses.
    # Installing dbt-core into Airflow's shared environment conflicts with
    # Airflow's own OpenTelemetry stack (protobuf<5 vs dbt-core's protobuf>=6),
    # confirmed the hard way while setting this DAG up. Cosmos's default LOCAL
    # execution mode + DBT_LS load mode both shell out to this path rather
    # than importing dbt-core as a library, so this works without needing
    # dbt-core in Airflow's own environment at all.
    dbt_executable_path="/opt/airflow/dbt/dbt_venv_clean/bin/dbt",
)

tpch_dbt_pipeline_cosmos = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    render_config=render_config,
    execution_config=execution_config,
    operator_args={"install_deps": False},  # must match render_config.dbt_deps â€” Cosmos validates these agree
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    dag_id="tpch_dbt_pipeline_cosmos",
    tags=["tpch", "dbt", "cosmos"],
    default_args={
        "on_success_callback": log_model_success,
        "on_failure_callback": log_model_failure,
    },
)


def _make_skip_gate(dag_id: str, original_pre_execute):
    """Wraps a task's existing pre_execute (if any) with a cycle-state check.
    Raising AirflowSkipException here marks the task SKIPPED without running
    it â€” the standard Airflow-native way to gate execution based on external
    state, without touching Cosmos's own operator internals."""

    def gated_pre_execute(self, context):
        model_name = _base_model_name(self.task_id)
        if get_cycle_status(dag_id, model_name) == "SUCCESS":
            raise AirflowSkipException(
                f"{model_name} already succeeded this cycle â€” skipping (zero-intervention restart)"
            )
        if original_pre_execute is not None:
            original_pre_execute(context)

    return gated_pre_execute


with tpch_dbt_pipeline_cosmos as dag:
    reset_task = dag.task_dict.get("reset_cycle")
    if reset_task is None:
        from airflow.operators.python import PythonOperator

        reset_task = PythonOperator(
            task_id="reset_cycle",
            python_callable=reset_cosmos_cycle,
            trigger_rule=TriggerRule.NONE_FAILED,
        )

    # Every Cosmos-generated task: patch pre_execute with the skip gate, and
    # relax trigger_rule to NONE_FAILED so a SKIPPED upstream (already done)
    # doesn't incorrectly cascade-skip downstream tasks that still need to run.
    for task in list(dag.tasks):
        if task.task_id == "reset_cycle":
            continue
        original = task.pre_execute if callable(getattr(task, "pre_execute", None)) else None
        task.pre_execute = types.MethodType(_make_skip_gate(dag.dag_id, original), task)
        task.trigger_rule = TriggerRule.NONE_FAILED

    # Wire reset_cycle downstream of every current leaf task (tasks with no
    # downstream dependents) so it only fires after the whole graph is done.
    leaves = [t for t in dag.tasks if not t.downstream_list and t.task_id != "reset_cycle"]
    for leaf in leaves:
        leaf >> reset_task
