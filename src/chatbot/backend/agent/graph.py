from __future__ import annotations

import logging
from functools import lru_cache

from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.agents.supervisor import build_supervisor
from src.chatbot.backend.agent.agents.retrieval_agent import build_retrieval_agent
from src.chatbot.backend.agent.agents.sql_agent import build_sql_agent
from src.chatbot.backend.agent.agents.analyst_agent import analyst_agent
from src.chatbot.backend.agent.agents.what_if_agent import what_if_agent

logger = logging.getLogger(__name__)


def _route_after_supervisor(state: AgentState) -> str:
    if state.get("intent") == "data_query":
        return "retrieval_agent"
    if state.get("intent") == "what_if_simulation":
        return "what_if_agent"
    return "end_early"


def build_graph() -> StateGraph:
    workflow = StateGraph(AgentState)

    workflow.add_node("supervisor", build_supervisor())
    workflow.add_node("retrieval_agent", build_retrieval_agent())
    workflow.add_node("sql_agent", build_sql_agent())
    workflow.add_node("analyst_agent", analyst_agent)
    workflow.add_node("what_if_agent", what_if_agent)

    workflow.set_entry_point("supervisor")

    workflow.add_conditional_edges(
        "supervisor",
        _route_after_supervisor,
        {
            "retrieval_agent": "retrieval_agent",
            "what_if_agent": "what_if_agent",
            "end_early": END,
        },
    )

    workflow.add_edge("retrieval_agent", "sql_agent")
    workflow.add_edge("sql_agent", "analyst_agent")
    workflow.add_edge("analyst_agent", END)
    workflow.add_edge("what_if_agent", END)

    return workflow


@lru_cache(maxsize=1)
def get_graph():
    logger.info("Compiling LangGraph agent graph...")
    return build_graph().compile()


def run_agent(question: str, chat_history: list = None) -> AgentState:
    graph = get_graph()
    initial_state: AgentState = {
        "question": question,
        "chat_history": chat_history or [],
        "retry_count": 0,
        "execution_log": [],
    }
    final_state = graph.invoke(initial_state)
    return final_state
