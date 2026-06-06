from langgraph.graph import StateGraph, END

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.agents.supervisor import build_supervisor
from src.chatbot.backend.agent.agents.retrieval_agent import build_retrieval_agent
from src.chatbot.backend.agent.agents.sql_agent import build_sql_agent
from src.chatbot.backend.agent.agents.analyst_agent import analyst_agent


def _route_after_supervisor(state: AgentState) -> str:
    if state.get("intent") == "data_query":
        return "retrieval_agent"
    return "end_early"


def build_graph() -> StateGraph:
    workflow = StateGraph(AgentState)

    workflow.add_node("supervisor", build_supervisor())
    workflow.add_node("retrieval_agent", build_retrieval_agent())
    workflow.add_node("sql_agent", build_sql_agent())
    workflow.add_node("analyst_agent", analyst_agent)

    workflow.set_entry_point("supervisor")

    workflow.add_conditional_edges(
        "supervisor",
        _route_after_supervisor,
        {
            "retrieval_agent": "retrieval_agent",
            "end_early": END,
        },
    )

    workflow.add_edge("retrieval_agent", "sql_agent")
    workflow.add_edge("sql_agent", "analyst_agent")
    workflow.add_edge("analyst_agent", END)

    return workflow


_compiled_graph = None


def get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = build_graph().compile()
    return _compiled_graph


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
