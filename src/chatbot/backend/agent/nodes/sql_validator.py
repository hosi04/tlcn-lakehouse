from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.trino_connector import trino_query

MAX_RETRIES = 3


def sql_validator(state: AgentState) -> AgentState:
    sql = state.get("sql", "").strip()
    retry_count = state.get("retry_count", 0)

    if not sql or not sql.lower().startswith("select"):
        return {
            "sql_error": "SQL không hợp lệ (phải bắt đầu bằng SELECT)",
            "retry_count": retry_count + 1,
            "query_result": [],
            "columns": [],
            "row_count": 0,
            "execution_log": state.get("execution_log", []) + [
                f"[sql_validator] Lỗi validation cơ bản: không phải SELECT"
            ],
        }

    try:
        df = trino_query(sql)
        records = df.to_dict(orient="records")
        return {
            "query_result": records,
            "columns": df.columns.tolist(),
            "row_count": len(records),
            "sql_error": None,
            "execution_log": state.get("execution_log", []) + [
                f"[sql_validator] SUCCESS — {len(records)} dòng"
            ],
        }
    except Exception as e:
        error_msg = str(e)
        return {
            "sql_error": error_msg,
            "retry_count": retry_count + 1,
            "query_result": [],
            "columns": [],
            "row_count": 0,
            "execution_log": state.get("execution_log", []) + [
                f"[sql_validator] FAILED (attempt {retry_count + 1}): {error_msg[:150]}"
            ],
        }


def should_retry(state: AgentState) -> str:
    sql_error = state.get("sql_error")
    retry_count = state.get("retry_count", 0)

    if not sql_error:
        return "success"
    if retry_count < MAX_RETRIES:
        return "retry"
    return "give_up"
