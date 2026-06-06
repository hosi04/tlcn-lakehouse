from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import ANALYST_PROMPT
from src.chatbot.backend.llm_connector import get_llm

_llm = None


def _get_llm():
    global _llm
    if _llm is None:
        _llm = get_llm()
    return _llm


def _summarize_result(rows: list[dict], columns: list[str], max_rows: int = 20) -> str:
    if not rows:
        return "Không có dữ liệu trả về."

    total = len(rows)
    sample = rows[:max_rows]

    lines = [f"Tổng: {total} dòng, {len(columns)} cột: {columns}"]
    lines.append("")

    for i, row in enumerate(sample):
        row_str = " | ".join(f"{k}={v}" for k, v in row.items())
        lines.append(f"  [{i+1}] {row_str}")

    if total > max_rows:
        lines.append(f"  ... và {total - max_rows} dòng nữa")

    return "\n".join(lines)


def analyst_agent(state: AgentState) -> AgentState:
    question = state.get("contextualized_question", state.get("question", ""))
    sql = state.get("sql", "")
    rows = state.get("query_result", [])
    columns = state.get("columns", [])
    sql_error = state.get("sql_error")
    log = state.get("execution_log", [])

    log = log + ["[analyst_agent] ▶ START"]

    if sql_error or not rows:
        analysis = (
            "❌ Không thể phân tích vì truy vấn SQL thất bại hoặc không có dữ liệu trả về."
            if sql_error
            else "📭 Truy vấn thành công nhưng không có dữ liệu phù hợp."
        )
        return {
            "analysis": analysis,
            "active_agent": "analyst_agent",
            "execution_log": log + [f"[analyst_agent] ✔ DONE — no data to analyze"],
        }

    result_summary = _summarize_result(rows, columns)

    prompt = ANALYST_PROMPT.format(
        question=question,
        sql=sql,
        row_count=len(rows),
        columns=", ".join(columns),
        result_summary=result_summary,
    )

    response = _get_llm().invoke(prompt).content.strip()

    return {
        "analysis": response,
        "active_agent": "analyst_agent",
        "execution_log": log + [
            f"[analyst_agent] Analyzed {len(rows)} rows, {len(columns)} columns",
            f"[analyst_agent] ✔ DONE",
        ],
    }
