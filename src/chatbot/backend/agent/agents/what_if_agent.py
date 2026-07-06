from __future__ import annotations

import json
import logging
import requests

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import WHAT_IF_PROMPT
from src.chatbot.backend.llm_connector import get_llm

logger = logging.getLogger(__name__)

SERVING_API_URL = "http://localhost:8001/predict"

def what_if_agent(state: AgentState) -> AgentState:
    logger.info("Executing What-If Simulation Agent via HTTP Serving API...")
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
        logger.info(f"Sending parameters to Serving API: {SERVING_API_URL}")
        resp = requests.post(SERVING_API_URL, json={"params": params}, timeout=10)
        resp.raise_for_status()
        
        data = resp.json()
        pred_revenue = data.get("prediction_brl", 0.0)
        rmse = data.get("rmse_benchmark", 0.0)
        lgb_version = data.get("model_version", "unknown")
        next_year = data.get("next_year", 2019)
        next_week = data.get("next_week", 1)

        summary = (
            f"🎯 **BÁO CÁO MÔ PHỎNG KỊCH BẢN QUẢN TRỊ (WHAT-IF SIMULATION)**\n\n"
            f"Dựa trên các chỉ tiêu KPI dự kiến cho tuần tới ({next_year}-W{next_week:02d}):\n"
        )
        for k, v in params.items():
            if v is not None:
                summary += f"- **{k}**: {v:,.0f}\n"

        summary += (
            f"\n📊 **Kết quả dự báo (Serving API — revenue_lightgbm v{lgb_version})**:\n"
            f"- **Doanh thu ước tính (Estimated Revenue)**: **{pred_revenue:,.2f} BRL**\n"
            f"- **Độ chính xác mô hình (RMSE Benchmark)**: {rmse:,.2f} BRL\n\n"
            f"💡 **Gợi ý chiến lược**: Kịch bản đầu vào cho thấy tiềm năng tăng trưởng tích cực. "
            f"Ban quản trị có thể cân nhắc phê duyệt ngân sách Marketing và bổ sung hàng hóa tại kho để đáp ứng mục tiêu này."
        )

        return {
            "active_agent": "what_if_agent",
            "direct_answer": summary,
            "execution_log": state.get("execution_log", []) + [
                log_msg, 
                f"[what_if_agent] Called Serving API successfully", 
                f"[what_if_agent] Predicted Revenue: {pred_revenue:,.2f} BRL"
            ],
        }

    except Exception as exc:
        logger.error("What-If simulation failed via Serving API: %s", exc, exc_info=True)
        return {
            "active_agent": "what_if_agent",
            "direct_answer": f"Xin lỗi, đã xảy ra lỗi khi kết nối tới Model Serving API. Vui lòng đảm bảo Server ML đang chạy trên port 8001.\nChi tiết lỗi: {exc}",
            "execution_log": state.get("execution_log", []) + [f"[what_if_agent] API Error: {exc}"],
        }
