from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import ANALYST_PROMPT
from src.chatbot.backend.llm_connector import get_llm


def _format_val(k: str, v: any) -> str:
    if isinstance(v, (int, float)) and not k.endswith(("_id", "_key", "year", "month", "day")):
        try:
            return f"{v:,.2f}"
        except Exception:
            return str(v)
    return str(v)


def _summarize_result(rows: list[dict], columns: list[str], max_rows: int = 20) -> str:
    if not rows:
        return "Không có dữ liệu trả về."

    total = len(rows)
    sample = rows[:max_rows]

    lines = [f"Tổng: {total} dòng, {len(columns)} cột: {columns}"]
    lines.append("")

    for i, row in enumerate(sample):
        row_str = " | ".join(f"{k}={_format_val(k, v)}" for k, v in row.items())
        lines.append(f"  [{i+1}] {row_str}")

    if total > max_rows:
        lines.append(f"  ... và {total - max_rows} dòng nữa")

    lines.append("\n[THỐNG KÊ NHANH TỪ HỆ THỐNG (DÀNH CHO BÁO CÁO)]:")
    num_cols = [
        c for c in columns
        if not c.endswith(("_id", "_key", "year", "month", "day", "date"))
        and any(isinstance(r.get(c), (int, float)) for r in rows)
    ]

    for col_name in num_cols:
        valid_vals = [r[col_name] for r in rows if isinstance(r.get(col_name), (int, float))]
        if valid_vals:
            total_val = sum(valid_vals)
            max_val = max(valid_vals)
            min_val = min(valid_vals)

            max_row = next((r for r in rows if r.get(col_name) == max_val), {})
            min_row = next((r for r in rows if r.get(col_name) == min_val), {})

            max_lbl = ", ".join(f"{k}={v}" for k, v in max_row.items() if k != col_name)
            min_lbl = ", ".join(f"{k}={v}" for k, v in min_row.items() if k != col_name)

            lines.append(
                f"- Cột '{col_name}': Tổng = {_format_val(col_name, total_val)} | "
                f"Đỉnh cao nhất (Max) = {_format_val(col_name, max_val)} ({max_lbl}) | "
                f"Thấp nhất (Min) = {_format_val(col_name, min_val)} ({min_lbl})"
            )

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
        if sql_error:
            import os
            import csv
            from datetime import datetime
            
            log_file = "logs/failed_queries.csv"
            os.makedirs("logs", exist_ok=True)
            file_exists = os.path.isfile(log_file)
            
            try:
                with open(log_file, mode='a', encoding='utf-8', newline='') as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(['timestamp', 'question', 'failed_sql', 'error_message'])
                    writer.writerow([datetime.now().strftime("%Y-%m-%d %H:%M:%S"), question, sql, sql_error])
            except Exception as e:
                log = log + [f"[analyst_agent] Failed to write log: {e}"]

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

    response = get_llm().invoke(prompt).content.strip()

    return {
        "analysis": response,
        "active_agent": "analyst_agent",
        "execution_log": log + [
            f"[analyst_agent] Analyzed {len(rows)} rows, {len(columns)} columns",
            f"[analyst_agent] ✔ DONE",
        ],
    }
