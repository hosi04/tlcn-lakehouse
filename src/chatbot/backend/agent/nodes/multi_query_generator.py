from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import MULTI_QUERY_PROMPT
from src.chatbot.backend.llm_connector import get_llm


def multi_query_generator(state: AgentState) -> AgentState:
    question = state.get("contextualized_question", state["question"])
    log = state.get("execution_log", [])

    prompt = MULTI_QUERY_PROMPT.format(question=question)
    raw = get_llm().invoke(prompt).content.strip()

    sub_queries = [
        line.strip().lstrip("0123456789.-) ")
        for line in raw.splitlines()
        if line.strip() and len(line.strip()) > 5
    ][:3]

    if not sub_queries:
        sub_queries = [question]

    return {
        "sub_queries": sub_queries,
        "execution_log": log + [
            f"[multi_query] Generated {len(sub_queries)} sub-queries:",
            *[f"  - {q}" for q in sub_queries],
        ],
    }
