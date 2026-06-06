from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.nodes.context_rewriter import context_rewriter
from src.chatbot.backend.agent.nodes.intent_classifier import intent_classifier


def _route_after_intent(state: AgentState) -> str:
    if state.get("intent") == "data_query":
        return "dispatch"
    return "end_early"


def _dispatch(state: AgentState) -> AgentState:
    return {
        "active_agent": "supervisor",
        "execution_log": state.get("execution_log", [])
        + ["[supervisor] Routing → retrieval_agent → sql_agent → analyst_agent"],
    }


def build_supervisor():
    graph = StateGraph(AgentState)

    graph.add_node("context_rewriter", context_rewriter)
    graph.add_node("intent_classifier", intent_classifier)
    graph.add_node("dispatch", _dispatch)

    graph.set_entry_point("context_rewriter")
    graph.add_edge("context_rewriter", "intent_classifier")

    graph.add_conditional_edges(
        "intent_classifier",
        _route_after_intent,
        {
            "dispatch": "dispatch",
            "end_early": END,
        },
    )
    graph.add_edge("dispatch", END)

    return graph.compile()
