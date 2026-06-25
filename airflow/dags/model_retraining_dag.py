import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

PROJECT_DIR = os.environ.get("LAKEHOUSE_PROJECT_DIR", "/opt/project")

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _evaluate_data_drift():
    if PROJECT_DIR not in sys.path:
        sys.path.insert(0, PROJECT_DIR)
    from src.mlops.revenue.drift_monitor import check_revenue_drift
    # Returns True if drift detected (proceeds to retrain), False if no drift (skips retrain)
    return check_revenue_drift()


with DAG(
    dag_id="mlops_monthly_retraining",
    default_args=default_args,
    schedule="@monthly",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["mlops", "lightgbm", "retrain", "lakehouse"],
) as dag:
    
    check_data_drift = ShortCircuitOperator(
        task_id="check_data_drift",
        python_callable=_evaluate_data_drift,
    )

    retrain_lightgbm_model = BashOperator(
        task_id="retrain_lightgbm_model",
        bash_command=f"cd {PROJECT_DIR} && python -m src.mlops.revenue.trainer_lightgbm",
    )

    check_data_drift >> retrain_lightgbm_model
