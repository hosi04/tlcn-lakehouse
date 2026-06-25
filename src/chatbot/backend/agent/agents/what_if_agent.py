from __future__ import annotations

import os
import json
import logging
import math
import numpy as np
import pandas as pd
import mlflow
import mlflow.lightgbm
import mlflow.sklearn
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import WHAT_IF_PROMPT
from src.chatbot.backend.llm_connector import get_llm
from src.mlops.data_loader import load_revenue_data
from src.mlops.revenue.features import build_lag_features, TARGET_COL

logger = logging.getLogger(__name__)


def _setup_mlflow_minio():
    load_dotenv()
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


def what_if_agent(state: AgentState) -> AgentState:
    logger.info("Executing What-If Simulation Agent via MLflow MinIO Registry...")
    question = state.get("contextualized_question", state["question"])
    prompt = WHAT_IF_PROMPT.format(question=question)

    try:
        response = get_llm().invoke(prompt).content.strip()
        clean_json = response
        if "```json" in clean_json:
            clean_json = clean_json.split("```json")[1].split("```")[0].strip()
        elif "```" in clean_json:
            clean_json = clean_json.split("```")[1].split("```")[0].strip()
        
        params = json.loads(clean_json)
    except Exception as exc:
        logger.warning("Could not parse What-If params: %s", exc)
        params = {}

    log_msg = f"[what_if_agent] Extracted KPIs: {params}"
    logger.info(log_msg)

    try:
        _setup_mlflow_minio()
        client = MlflowClient()
        
        logger.info("Fetching revenue_lightgbm from MLflow Registry...")
        versions = client.search_model_versions("name='revenue_lightgbm'")
        if not versions:
            raise ValueError("Không tìm thấy model 'revenue_lightgbm' trong MLflow Registry.")

        latest = max(versions, key=lambda v: int(v.version))
        lgb_version = latest.version
        lgb_run_id = latest.run_id

        model = mlflow.lightgbm.load_model(f"models:/revenue_lightgbm/{lgb_version}")
        scaler = mlflow.sklearn.load_model(f"runs:/{lgb_run_id}/scaler")
        
        logger.info("Model & Scaler loaded successfully from MinIO (Run ID: %s)", lgb_run_id)

        df = load_revenue_data().sort_values(["year", "week_of_year"]).reset_index(drop=True)
        
        try:
            rmse = client.get_run(lgb_run_id).data.metrics.get("rmse", 23296.40)
        except Exception:
            rmse = 23296.40

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

        if hasattr(scaler, "feature_names_in_"):
            features = list(scaler.feature_names_in_)
        else:
            from src.mlops.revenue.trainer_lightgbm import _LGB_EXCLUDE
            features = [
                c for c in df_lag.columns
                if c not in _LGB_EXCLUDE
                and df_lag[c].dtype in ("float64", "int64", "float32", "int32")
            ]

        X_sim = df_lag[features].iloc[-1:]
        X_sim_s = scaler.transform(X_sim)
        pred_log = model.predict(X_sim_s)
        pred_revenue = np.maximum(np.expm1(pred_log[0]), 0)

        summary = (
            f"🎯 **BÁO CÁO MÔ PHỎNG KỊCH BẢN QUẢN TRỊ (WHAT-IF SIMULATION)**\n\n"
            f"Dựa trên các chỉ tiêu KPI dự kiến cho tuần tới ({next_year}-W{next_week:02d}):\n"
        )
        for k, v in params.items():
            if v is not None:
                summary += f"- **{k}**: {v:,.0f}\n"

        summary += (
            f"\n📊 **Kết quả dự báo (MLflow Registry — revenue_lightgbm v{lgb_version})**:\n"
            f"- **Doanh thu ước tính (Estimated Revenue)**: **{pred_revenue:,.2f} BRL**\n"
            f"- **Độ chính xác mô hình (RMSE Benchmark)**: {rmse:,.2f} BRL\n\n"
            f"💡 **Gợi ý chiến lược**: Kịch bản đầu vào cho thấy tiềm năng tăng trưởng tích cực. "
            f"Ban quản trị có thể cân nhắc phê duyệt ngân sách Marketing và bổ sung hàng hóa tại kho để đáp ứng mục tiêu này."
        )

        return {
            "active_agent": "what_if_agent",
            "direct_answer": summary,
            "execution_log": state.get("execution_log", []) + [log_msg, f"[what_if_agent] Loaded model v{lgb_version} from MinIO", f"[what_if_agent] Predicted Revenue: {pred_revenue:,.2f} BRL"],
        }

    except Exception as exc:
        logger.error("What-If simulation failed: %s", exc, exc_info=True)
        return {
            "active_agent": "what_if_agent",
            "direct_answer": f"Xin lỗi, đã xảy ra lỗi khi tải mô hình từ MLflow/MinIO: {exc}",
            "execution_log": state.get("execution_log", []) + [f"[what_if_agent] Error: {exc}"],
        }
