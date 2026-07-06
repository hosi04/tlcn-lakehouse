import os
import logging
from contextlib import asynccontextmanager

import numpy as np
import pandas as pd
import mlflow
import mlflow.lightgbm
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv

from src.mlops.data_loader import load_revenue_data
from src.mlops.revenue.features import build_lag_features

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# --- Global Variables ---
ml_model = None
ml_scaler = None
ml_version = None
historical_df = None
rmse_benchmark = 23296.40

def _setup_mlflow_minio():
    minio_endpoint = os.getenv("MINIO_ENDPOINT")
    if minio_endpoint:
        os.environ["MLFLOW_S3_ENDPOINT_URL"] = f"http://{minio_endpoint}"
    minio_access_key = os.getenv("MINIO_ACCESS_KEY")
    if minio_access_key:
        os.environ["AWS_ACCESS_KEY_ID"] = minio_access_key
    minio_secret_key = os.getenv("MINIO_SECRET_KEY")
    if minio_secret_key:
        os.environ["AWS_SECRET_ACCESS_KEY"] = minio_secret_key
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
    if tracking_uri:
        mlflow.set_tracking_uri(tracking_uri)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global ml_model, ml_scaler, ml_version, historical_df, rmse_benchmark
    logger.info("[Serving] Khởi động Model Serving API...")
    
    _setup_mlflow_minio()
    client = MlflowClient()
    
    logger.info("[Serving] Đang tìm mô hình 'revenue_lightgbm' mới nhất từ MLflow...")
    versions = client.search_model_versions("name='revenue_lightgbm'")
    if not versions:
        logger.error("Không tìm thấy model 'revenue_lightgbm' trong MLflow Registry.")
        raise RuntimeError("Model not found in MLflow.")
        
    latest = max(versions, key=lambda v: int(v.version))
    ml_version = latest.version
    lgb_run_id = latest.run_id

    logger.info(f"[Serving] Tải model phiên bản v{ml_version} (Run ID: {lgb_run_id})...")
    ml_model = mlflow.lightgbm.load_model(f"models:/revenue_lightgbm/{ml_version}")
    ml_scaler = mlflow.sklearn.load_model(f"runs:/{lgb_run_id}/scaler")
    
    try:
        rmse_benchmark = client.get_run(lgb_run_id).data.metrics.get("rmse", 23296.40)
    except Exception:
        pass

    logger.info("[Serving] Đang khởi động PySpark để nạp lịch sử dữ liệu (Chỉ chạy 1 lần!)...")
    df = load_revenue_data()
    historical_df = df.sort_values(["year", "week_of_year"]).reset_index(drop=True)
    logger.info(f"[Serving] Đã nạp thành công {len(historical_df)} tuần dữ liệu. Sẵn sàng phục vụ!")
    
    yield
    
    logger.info("[Serving] Đóng ứng dụng Model Serving.")


app = FastAPI(title="ML Model Serving API", lifespan=lifespan)

class WhatIfRequest(BaseModel):
    params: dict


@app.post("/predict")
def predict_what_if(req: WhatIfRequest):
    if historical_df is None or ml_model is None or ml_scaler is None:
        raise HTTPException(status_code=503, detail="Mô hình chưa sẵn sàng.")
        
    params = req.params
    df = historical_df.copy()
    
    last_row = df.iloc[-1].copy()
    next_week = int(last_row["week_of_year"]) + 1
    next_year = int(last_row["year"])
    if next_week > 52:
        next_week = 1
        next_year += 1

    new_row = last_row.copy()
    new_row["year"] = next_year
    new_row["week_of_year"] = next_week

    for k, v in params.items():
        if v is not None and k in new_row.index:
            new_row[k] = float(v)

    df_sim = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    df_lag = build_lag_features(df_sim)

    if hasattr(ml_scaler, "feature_names_in_"):
        features = list(ml_scaler.feature_names_in_)
    else:
        from src.mlops.revenue.trainer_lightgbm import _LGB_EXCLUDE
        features = [
            c for c in df_lag.columns
            if c not in _LGB_EXCLUDE
            and df_lag[c].dtype in ("float64", "int64", "float32", "int32")
        ]

    X_sim = df_lag[features].iloc[-1:]
    X_sim_s = ml_scaler.transform(X_sim)
    pred_log = ml_model.predict(X_sim_s)
    pred_revenue = float(np.maximum(np.expm1(pred_log[0]), 0))

    return {
        "success": True,
        "prediction_brl": pred_revenue,
        "rmse_benchmark": rmse_benchmark,
        "model_version": ml_version,
        "next_year": next_year,
        "next_week": next_week
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
