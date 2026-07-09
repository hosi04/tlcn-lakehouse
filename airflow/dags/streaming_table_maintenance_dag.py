import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = os.environ.get("LAKEHOUSE_PROJECT_DIR", "/opt/project")

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="iceberg_streaming_table_maintenance",
    default_args=default_args,
    schedule="*/20 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["iceberg", "maintenance", "streaming", "lakehouse"],
) as dag:

    compaction_and_vacuum = BashOperator(
        task_id="compaction_and_vacuum_20min",
        bash_command=f"cd {PROJECT_DIR} && python -m src.etl.maintenance.iceberg_maintenance streaming",
    )

    compaction_and_vacuum
