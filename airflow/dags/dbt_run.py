"""
Standalone dbt Execution DAG
============================
Runs dbt models on-demand or on schedule.
Can be triggered independently or via the main pipeline.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DEFAULT_ARGS = {
    "owner": "analytics-team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="dbt_transformations",
    default_args=DEFAULT_ARGS,
    description="Run dbt models and tests",
    schedule_interval=None,  # Triggered by main DAG or manually
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "transformations", "snowflake"],
) as dag:

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /opt/airflow/dbt && dbt deps --profiles-dir .",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="""
            cd /opt/airflow/dbt &&             dbt run --profiles-dir . --target prod --full-refresh
        """,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt && dbt test --profiles-dir . --target prod",
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/airflow/dbt && dbt docs generate --profiles-dir . --target prod",
    )

    dbt_deps >> dbt_run >> dbt_test >> dbt_docs_generate
