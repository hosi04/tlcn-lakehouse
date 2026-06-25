from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

import mlflow
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EXPERIMENT_NAME = "P1_Revenue_Forecasting"
RESULTS_DIR = Path("experiments/results")
MODELS = ["lightgbm", "lstm", "prophet"]


def run_ml_benchmark():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    mlflow_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    mlflow.set_tracking_uri(mlflow_uri)

    logger.info("Đang load kết quả từ MLflow experiment: %s", EXPERIMENT_NAME)

    client = mlflow.MlflowClient()
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)

    if not experiment:
        logger.error("❌ Experiment '%s' chưa tồn tại trên MLflow (%s).", EXPERIMENT_NAME, mlflow_uri)
        logger.error("   Hãy chạy: python -m src.mlops.revenue.trainer_lightgbm")
        return False

    runs = client.search_runs(experiment_ids=[experiment.experiment_id])

    print("\n" + "=" * 80)
    print(" ML MODEL BENCHMARK — Weekly Revenue Forecasting ".center(80, "="))
    print("=" * 80)
    print(f"  {'Model':<18} {'RMSE':>13} {'MAE':>12} {'MAPE':>10}   {'Run ID':<32}")
    print("  " + "-" * 76)

    results = []
    for model_type in MODELS:
        model_runs = [
            r for r in runs
            if r.data.params.get("model_type") == model_type and "rmse" in r.data.metrics
        ]
        if not model_runs:
            model_runs = [
                r for r in runs
                if model_type in r.info.run_name.lower() and "rmse" in r.data.metrics
            ]

        if model_runs:
            best_run = min(model_runs, key=lambda r: r.data.metrics["rmse"])
            rmse = best_run.data.metrics.get("rmse", 0)
            mae = best_run.data.metrics.get("mae", 0)
            mape = best_run.data.metrics.get("mape", None)
            run_id = best_run.info.run_id

            rmse_str = f"{rmse:,.0f}" if rmse else "N/A"
            mae_str = f"{mae:,.0f}" if mae else "N/A"
            mape_str = f"{mape:.1f}%" if mape is not None else "N/A"
            run_id_str = run_id
            status = "OK"
        else:
            rmse, mae, mape = -1, -1, -1
            rmse_str, mae_str, mape_str = "N/A", "N/A", "N/A"
            run_id_str = "Chưa train"
            status = "MISSING"

        print(f"  {model_type.capitalize():<18} {rmse_str:>13} {mae_str:>12} {mape_str:>10}   {run_id_str:<32}")
        results.append({
            "model": model_type,
            "rmse": rmse if rmse != -1 else "",
            "mae": mae if mae != -1 else "",
            "mape": mape if mape != -1 else "",
            "run_id": run_id_str if status == "OK" else "",
            "status": status,
        })

    print("=" * 80 + "\n")

    csv_path = RESULTS_DIR / "ml_benchmark.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    logger.info("ML benchmark saved → %s", csv_path)
    logger.info("   MLflow UI: %s", mlflow_uri)
    return any(r["status"] == "OK" for r in results)


if __name__ == "__main__":
    raise SystemExit(0 if run_ml_benchmark() else 1)
