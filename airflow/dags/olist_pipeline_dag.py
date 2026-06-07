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
    dag_id="olist_bronze_silver_gold",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["olist", "lakehouse"],
) as dag:
    bronze_assets = BashOperator(
        task_id="bronze_assets",
        bash_command=f"cd {PROJECT_DIR} && python -m src.etl.bronze.bronze_assets",
    )

    silver_assets = BashOperator(
        task_id="silver_assets",
        bash_command=f"cd {PROJECT_DIR} && python -m src.etl.silver.silver_assets",
    )

    gold_assets = BashOperator(
        task_id="gold_assets",
        bash_command=f"cd {PROJECT_DIR} && python -m src.etl.gold.gold_assets",
    )

    bronze_assets >> silver_assets >> gold_assets
