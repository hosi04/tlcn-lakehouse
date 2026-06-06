from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.lakehouse_tools import execute_sql

MAX_RETRIES = 3


def sql_validator(state: AgentState) -> AgentState:
    sql = state.get("sql", "").strip()
    retry_count = state.get("retry_count", 0)

    result = execute_sql(sql)
    if result.get("success"):
        records = result.get("rows", [])
        truncated = result.get("truncated", False)
        suffix = " (truncated)" if truncated else ""
        return {
            "query_result": records,
            "columns": result.get("columns", []),
            "row_count": len(records),
            "sql_error": None,
            "execution_log": state.get("execution_log", []) + [
                f"[sql_validator] SUCCESS — {len(records)} dòng{suffix}"
            ],
        }

    error_msg = result.get("error", "SQL validation/execution failed")
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
