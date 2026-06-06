from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.nodes.multi_query_generator import multi_query_generator
from src.chatbot.backend.agent.nodes.schema_retriever_node import schema_retriever
from src.chatbot.backend.agent.nodes.metadata_fetcher import metadata_fetcher
from src.chatbot.backend.agent.nodes.column_pruner import column_pruner


def _tag_entry(state: AgentState) -> AgentState:
    return {
        "active_agent": "retrieval_agent",
        "execution_log": state.get("execution_log", [])
        + ["[retrieval_agent] ▶ START"],
    }


def _tag_exit(state: AgentState) -> AgentState:
    return {
        "execution_log": state.get("execution_log", [])
        + ["[retrieval_agent] ✔ DONE"],
    }


def build_retrieval_agent():
    graph = StateGraph(AgentState)

    graph.add_node("tag_entry", _tag_entry)
    graph.add_node("multi_query", multi_query_generator)
    graph.add_node("schema_retriever", schema_retriever)
    graph.add_node("metadata_fetcher", metadata_fetcher)
    graph.add_node("column_pruner", column_pruner)
    graph.add_node("tag_exit", _tag_exit)

    graph.set_entry_point("tag_entry")
    graph.add_edge("tag_entry", "multi_query")
    graph.add_edge("multi_query", "schema_retriever")
    graph.add_edge("schema_retriever", "metadata_fetcher")
    graph.add_edge("metadata_fetcher", "column_pruner")
    graph.add_edge("column_pruner", "tag_exit")
    graph.add_edge("tag_exit", END)

    return graph.compile()
