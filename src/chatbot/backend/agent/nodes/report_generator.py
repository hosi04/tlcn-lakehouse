import json
from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import REPORT_PROMPT
from src.chatbot.backend.llm_connector import get_llm

_llm = get_llm()
_MAX_ROWS_IN_PROMPT = 20


def _format_data_text(rows: list[dict], columns: list[str], max_rows: int) -> str:
    if not rows:
        return "(Không có dữ liệu)"

    sample = rows[:max_rows]
    header = " | ".join(columns)
    sep = "-" * len(header)
    data_lines = [header, sep]
    for row in sample:
        data_lines.append(" | ".join(str(row.get(c, "")) for c in columns))

    if len(rows) > max_rows:
        data_lines.append(f"... (còn {len(rows) - max_rows} dòng nữa)")

    return "\n".join(data_lines)


def report_generator(state: AgentState) -> AgentState:
    question = state["question"]
    sql = state.get("sql", "")
    query_result = state.get("query_result", [])
    columns = state.get("columns", [])
    row_count = state.get("row_count", 0)
    sql_error = state.get("sql_error")

    if sql_error and not query_result:
        report = (
            f"⚠️ **Không thể truy vấn dữ liệu**\n\n"
            f"Hệ thống không thể sinh câu truy vấn hợp lệ sau {3} lần thử.\n\n"
            f"**Lỗi cuối:** `{sql_error[:200]}`\n\n"
            f"Vui lòng thử diễn đạt lại câu hỏi cụ thể hơn."
        )
        return {
            "report": report,
            "execution_log": state.get("execution_log", [])
            + ["[report_generator] Tạo báo cáo lỗi"],
        }

    data_text = _format_data_text(query_result, columns, _MAX_ROWS_IN_PROMPT)
    prompt = REPORT_PROMPT.format(
        question=question,
        sql=sql,
        row_count=row_count,
        data_text=data_text,
    )

    report = _llm.invoke(prompt).content.strip()

    return {
        "report": report,
        "execution_log": state.get("execution_log", [])
        + [f"[report_generator] Báo cáo {len(report)} ký tự"],
    }
