import json
from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import VIZ_SELECT_PROMPT
from src.chatbot.backend.llm_connector import get_llm

_llm = get_llm()

_DEFAULT_CHART = {
    "chart_type": "table",
    "x": None,
    "y": None,
    "color": None,
    "title": "Kết quả truy vấn",
    "x_label": None,
    "y_label": None,
}


def _format_data_sample(rows: list[dict], n: int = 5) -> str:
    if not rows:
        return "(Không có dữ liệu)"
    sample = rows[:n]
    lines = []
    for i, row in enumerate(sample, 1):
        lines.append(f"Row {i}: " + ", ".join(f"{k}={v}" for k, v in row.items()))
    return "\n".join(lines)


def visualization_selector(state: AgentState) -> AgentState:
    query_result = state.get("query_result", [])
    columns = state.get("columns", [])
    row_count = state.get("row_count", 0)
    question = state["question"]

    # Nếu không có data, dùng table mặc định
    if not query_result or not columns:
        return {
            "chart_config": _DEFAULT_CHART,
            "execution_log": state.get("execution_log", [])
            + ["[viz_selector] Không có data → table mặc định"],
        }

    # Nếu chỉ có 1 row và 1 cột → KPI card
    if row_count == 1 and len(columns) == 1:
        val = list(query_result[0].values())[0]
        return {
            "chart_config": {
                "chart_type": "kpi",
                "x": None,
                "y": columns[0],
                "color": None,
                "title": question[:60],
                "x_label": None,
                "y_label": None,
                "kpi_value": val,
            },
            "execution_log": state.get("execution_log", [])
            + ["[viz_selector] 1 giá trị → KPI card"],
        }

    data_sample = _format_data_sample(query_result)
    prompt = VIZ_SELECT_PROMPT.format(
        question=question,
        columns=", ".join(columns),
        row_count=row_count,
        data_sample=data_sample,
    )

    try:
        response = _llm.invoke(prompt).content.strip()
        # Trích xuất JSON từ response
        if "```" in response:
            response = response.split("```")[1].strip()
            if response.startswith("json"):
                response = response[4:].strip()

        chart_config = json.loads(response)
        log_msg = f"[viz_selector] chart_type={chart_config.get('chart_type')}"
    except Exception as e:
        chart_config = {**_DEFAULT_CHART, "title": question[:60]}
        log_msg = f"[viz_selector] Parse lỗi ({e}), dùng table mặc định"

    return {
        "chart_config": chart_config,
        "execution_log": state.get("execution_log", []) + [log_msg],
    }
