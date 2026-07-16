"""
tpch_dbt_pipeline_plain
------------------------
Bronze -> Silver -> Snapshot -> Gold -> Reset, one Airflow task per layer
(BashOperator-less: a PythonOperator wraps the dbt call so we can inspect
run_results.json and log to Snowflake before deciding pass/fail).

Restartability + repeatability (works for BOTH a task Clear AND a fresh
`airflow dags trigger`, and supports running the DAG multiple times a day):
  - Each layer checks its own run_results.json before doing anything:
    never run -> full `dbt build`; last attempt had an error/skip -> `dbt
    retry` (only reruns what's still pending); last attempt was fully clean
    -> SKIP that layer entirely (no dbt call).
  - If a layer fails, downstream layers don't run (task dependency), and the
    failure state is preserved. The NEXT trigger â€” Clear or fresh â€” will SKIP
    every layer that already succeeded and RETRY only the one that failed,
    then continue downstream automatically.
  - If ALL FOUR layers succeed, the final `reset_cycle` task wipes every
    layer's run_results.json. That's what makes a fully-successful run mean
    "the next trigger starts over from bronze" rather than skipping forever â€”
    important if you trigger this DAG more than once a day.

Layers: bronze -> silver -> snapshot -> gold. The snapshot step runs
snapshots/tpch (dim_customer / dim_supplier SCD2 sources) after silver and
before gold, since gold's SCD2 dims read from those snapshot tables.

Requires:
  - No separate Airflow connection needed â€” Snowflake logging reuses the
    same key-pair auth as dbt's profiles.yml directly.
  - Selection is path-based (LAYER_SELECT in dbt_airflow_utils.py), matching
    the project's folder-config layout â€” no dbt tags needed.
  - dbt installed directly in the Airflow containers (not the host dbt_venv).
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.dbt_airflow_utils import execute_layer, reset_cycle

default_args = {
    "owner": "midhun",
    "retries": 0,  # manual "Clear" is the intended restart path; bump to 1-2 if you want auto-retry
}

with DAG(
    dag_id="tpch_dbt_pipeline_plain",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=["tpch", "dbt", "plain"],
) as dag:

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=execute_layer,
        op_kwargs={"layer": "bronze"},
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=execute_layer,
        op_kwargs={"layer": "silver"},
    )

    # dim_customer / dim_supplier (SCD2) are built from these snapshots, which
    # read off silver â€” must run after silver, before gold.
    snapshot = PythonOperator(
        task_id="snapshot",
        python_callable=execute_layer,
        op_kwargs={"layer": "snapshot"},
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=execute_layer,
        op_kwargs={"layer": "gold"},
    )

    # Only runs if bronze, silver, snapshot, gold ALL succeeded (default trigger
    # rule = all_success). Wipes state so the next trigger reprocesses everything
    # fresh, instead of skipping layers forever. If anything upstream failed,
    # this task never runs, so the failure state is preserved for the next
    # trigger to resume from.
    reset = PythonOperator(
        task_id="reset_cycle",
        python_callable=reset_cycle,
    )

    bronze >> silver >> snapshot >> gold >> reset
