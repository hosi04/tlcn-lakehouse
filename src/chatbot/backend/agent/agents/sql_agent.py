from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.nodes.sql_generator import sql_generator
from src.chatbot.backend.agent.nodes.sql_validator import sql_validator, should_retry


def _tag_entry(state: AgentState) -> AgentState:
    return {
        "active_agent": "sql_agent",
        "execution_log": state.get("execution_log", [])
        + ["[sql_agent] ▶ START"],
    }


def _tag_exit(state: AgentState) -> AgentState:
    retry = state.get("retry_count", 0)
    error = state.get("sql_error")
    status = "SUCCESS" if not error else f"GIVE_UP after {retry} retries"
    return {
        "execution_log": state.get("execution_log", [])
        + [f"[sql_agent] ✔ DONE — {status}"],
    }


def _route_after_validation(state: AgentState) -> str:
    result = should_retry(state)
    if result == "retry":
        return "sql_generator"
    return "tag_exit"


def build_sql_agent():
    graph = StateGraph(AgentState)

    graph.add_node("tag_entry", _tag_entry)
    graph.add_node("sql_generator", sql_generator)
    graph.add_node("sql_validator", sql_validator)
    graph.add_node("tag_exit", _tag_exit)

    graph.set_entry_point("tag_entry")
    graph.add_edge("tag_entry", "sql_generator")
    graph.add_edge("sql_generator", "sql_validator")

    graph.add_conditional_edges(
        "sql_validator",
        _route_after_validation,
        {
            "sql_generator": "sql_generator",
            "tag_exit": "tag_exit",
        },
    )
    graph.add_edge("tag_exit", END)

    return graph.compile()
