"""
Real-Time Analytics ETL Pipeline
================================
Orchestrates the full data pipeline:
1. Produce events to Kafka (R script)
2. Run Spark Streaming (R script)
3. Run Spark Batch (R script)
4. Execute dbt models
5. Generate R Markdown report
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "analytics-team",
    "depends_on_past": False,
    "email": ["data-team@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

SNOWFLAKE_CONN_ID = "snowflake_default"

# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="realtime_analytics_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end real-time analytics pipeline with R, Spark, Kafka, dbt, and Snowflake",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["realtime", "analytics", "r", "spark", "kafka", "dbt", "snowflake"],
    max_active_runs=1,
    doc_md="""
    ### Real-Time Analytics Pipeline

    This DAG orchestrates:
    - **Kafka Event Production** (R)
    - **Spark Streaming** (sparklyr)
    - **Spark Batch Processing** (sparklyr)
    - **dbt Transformations**
    - **R Markdown Report Generation**

    **Schedule**: Hourly
    **Owner**: Analytics Team
    """,
) as dag:

    # =====================================================================
    # Task 1: Data Ingestion - Produce Kafka Events
    # =====================================================================
    produce_events = BashOperator(
        task_id="produce_kafka_events",
        bash_command="""
            Rscript /opt/airflow/r_analytics/utils/kafka_connector.R                 --mode=produce                 --bootstrap-servers=kafka:9092                 --topic=user_events                 --batch-size=1000
        """,
        env={
            "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
            "R_LOG_LEVEL": "INFO",
        },
    )

    # =====================================================================
    # Task 2: Spark Streaming (runs for 5 minutes then checkpoints)
    # =====================================================================
    spark_streaming = BashOperator(
        task_id="spark_streaming_processing",
        bash_command="""
            timeout 300             /opt/spark/bin/spark-submit                 --master spark://spark-master:7077                 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0                 --driver-memory 2g                 --executor-memory 2g                 /opt/spark/work-dir/streaming_processing.R                 --kafka-bootstrap kafka:9092                 --snowflake-url "$SNOWFLAKE_ACCOUNT"                 --snowflake-db "$SNOWFLAKE_DATABASE"                 --snowflake-schema "$SNOWFLAKE_SCHEMA"
        """,
        env={
            "SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "SNOWFLAKE_DATABASE": "{{ var.value.snowflake_database }}",
            "SNOWFLAKE_SCHEMA": "{{ var.value.snowflake_schema }}",
            "SNOWFLAKE_WAREHOUSE": "{{ var.value.snowflake_warehouse }}",
        },
    )

    # =====================================================================
    # Task 3: Spark Batch Processing
    # =====================================================================
    spark_batch = BashOperator(
        task_id="spark_batch_processing",
        bash_command="""
            /opt/spark/bin/spark-submit                 --master spark://spark-master:7077                 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,net.snowflake:snowflake-jdbc:3.14.3,net.snowflake:spark-snowflake_2.12:2.12.0                 --driver-memory 4g                 --executor-memory 4g                 --executor-cores 2                 /opt/spark/work-dir/batch_processing.R                 --run-date {{ ds }}                 --snowflake-db "$SNOWFLAKE_DATABASE"
        """,
        env={
            "SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "SNOWFLAKE_DATABASE": "{{ var.value.snowflake_database }}",
            "SNOWFLAKE_SCHEMA": "{{ var.value.snowflake_schema }}",
            "SNOWFLAKE_WAREHOUSE": "{{ var.value.snowflake_warehouse }}",
        },
    )

    # =====================================================================
    # Task 4: Data Quality Check (Snowflake)
    # =====================================================================
    data_quality_check = SnowflakeOperator(
        task_id="data_quality_check",
        sql="""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT user_id) as unique_users,
                MAX(event_timestamp) as latest_event
            FROM {{ var.value.snowflake_database }}.{{ var.value.snowflake_schema }}.stg_events
            WHERE event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            HAVING COUNT(*) > 0
        """,
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
    )

    # =====================================================================
    # Task 5: dbt Transformations
    # =====================================================================
    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command="""
            cd /opt/airflow/dbt &&             dbt deps --profiles-dir . &&             dbt run                 --profiles-dir .                 --target prod                 --vars '{"run_date": "{{ ds }}"}'                 --full-refresh
        """,
        env={
            "DBT_SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "DBT_SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "DBT_SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "DBT_SNOWFLAKE_DATABASE": "{{ var.value.snowflake_database }}",
            "DBT_SNOWFLAKE_SCHEMA": "{{ var.value.snowflake_schema }}",
            "DBT_SNOWFLAKE_WAREHOUSE": "{{ var.value.snowflake_warehouse }}",
        },
    )

    # =====================================================================
    # Task 6: dbt Tests
    # =====================================================================
    dbt_test = BashOperator(
        task_id="dbt_run_tests",
        bash_command="""
            cd /opt/airflow/dbt &&             dbt test                 --profiles-dir .                 --target prod                 --vars '{"run_date": "{{ ds }}"}'
        """,
        env={
            "DBT_SNOWFLAKE_ACCOUNT": "{{ var.value.snowflake_account }}",
            "DBT_SNOWFLAKE_USER": "{{ var.value.snowflake_user }}",
            "DBT_SNOWFLAKE_PASSWORD": "{{ var.value.snowflake_password }}",
            "DBT_SNOWFLAKE_DATABASE": "{{ var.value.snowflake_database }}",
            "DBT_SNOWFLAKE_SCHEMA": "{{ var.value.snowflake_schema }}",
            "DBT_SNOWFLAKE_WAREHOUSE": "{{ var.value.snowflake_warehouse }}",
        },
    )

    # =====================================================================
    # Task 7: Generate R Markdown Report
    # =====================================================================
    generate_report = BashOperator(
        task_id="generate_daily_report",
        bash_command="""
            Rscript -e "
                rmarkdown::render(
                    input = '/opt/airflow/r_analytics/reports/daily_report.Rmd',
                    output_file = '/opt/airflow/output/daily_report_{{ ds }}.html',
                    params = list(run_date = '{{ ds }}')
                )
            "
        """,
    )

    # =====================================================================
    # Task 8: Cleanup & Notifications
    # =====================================================================
    def _notify_success(context):
        """Send success notification."""
        ti = context["ti"]
        logging.info(f"Pipeline completed successfully for {ti.execution_date}")

    def _notify_failure(context):
        """Send failure alert."""
        ti = context["ti"]
        logging.error(f"Pipeline failed at task {ti.task_id} for {ti.execution_date}")

    # =====================================================================
    # Dependencies
    # =====================================================================
    produce_events >> spark_streaming >> spark_batch >> data_quality_check >> dbt_run >> dbt_test >> generate_report
