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
    dag_id="events_silver_gold",
    default_args=default_args,
    schedule="*/15 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["events", "lakehouse"],
) as dag:
    silver_events = BashOperator(
        task_id="silver_events",
        bash_command=f"cd {PROJECT_DIR} && python -m src.spark.silver.silver_events",
    )

    gold_events = BashOperator(
        task_id="gold_events",
        bash_command=f"cd {PROJECT_DIR} && python -m src.spark.gold.gold_events",
    )

    silver_events >> gold_events