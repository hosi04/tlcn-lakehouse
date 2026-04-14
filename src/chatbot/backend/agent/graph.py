from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.nodes.intent_classifier import intent_classifier
from src.chatbot.backend.agent.nodes.schema_retriever_node import schema_retriever
from src.chatbot.backend.agent.nodes.metadata_fetcher import metadata_fetcher
from src.chatbot.backend.agent.nodes.column_pruner import column_pruner
from src.chatbot.backend.agent.nodes.sql_generator import sql_generator
from src.chatbot.backend.agent.nodes.sql_validator import sql_validator, should_retry
from src.chatbot.backend.agent.nodes.visualization_selector import visualization_selector
from src.chatbot.backend.agent.nodes.report_generator import report_generator


def route_after_intent(state: AgentState) -> str:
    if state.get("intent") == "data_query":
        return "schema_retriever"
    return "end_early"


def route_after_validation(state: AgentState) -> str:
    result = should_retry(state)
    if result == "retry":
        return "sql_generator"    # Quay lại generate với error context
    elif result == "success":
        return "visualization_selector"
    else:
        return "report_generator"  # give_up → tạo báo cáo lỗi


def build_graph() -> StateGraph:
    workflow = StateGraph(AgentState)
    workflow.add_node("intent_classifier", intent_classifier)
    workflow.add_node("schema_retriever", schema_retriever)
    workflow.add_node("metadata_fetcher", metadata_fetcher)
    workflow.add_node("column_pruner", column_pruner)
    workflow.add_node("sql_generator", sql_generator)
    workflow.add_node("sql_validator", sql_validator)
    workflow.add_node("visualization_selector", visualization_selector)
    workflow.add_node("report_generator", report_generator)

    workflow.set_entry_point("intent_classifier")

    workflow.add_conditional_edges(
        "intent_classifier",
        route_after_intent,
        {
            "schema_retriever": "schema_retriever",
            "end_early": END,
        },
    )

    workflow.add_edge("schema_retriever", "metadata_fetcher")
    workflow.add_edge("metadata_fetcher", "column_pruner")
    workflow.add_edge("column_pruner", "sql_generator")
    workflow.add_edge("sql_generator", "sql_validator")

    workflow.add_conditional_edges(
        "sql_validator",
        route_after_validation,
        {
            "sql_generator": "sql_generator",         # retry
            "visualization_selector": "visualization_selector",  # success
            "report_generator": "report_generator",   # give_up
        },
    )

    workflow.add_edge("visualization_selector", "report_generator")
    workflow.add_edge("report_generator", END)

    return workflow


_compiled_graph = None


def get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_graph().compile()
    return _compiled_graph


def run_agent(question: str) -> AgentState:
    """
    Chạy toàn bộ NL2SQL agent pipeline.

    Args:
        question: Câu hỏi tiếng Việt của người dùng

    Returns:
        AgentState dict với đầy đủ: sql, query_result, chart_config, report
    """
    graph = get_graph()
    initial_state: AgentState = {
        "question": question,
        "retry_count": 0,
        "execution_log": [],
    }
    final_state = graph.invoke(initial_state)
    return final_state
