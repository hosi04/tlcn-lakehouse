from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.retrieval.schema_retriever import (
    retrieve_tables,
    retrieve_tables_with_scores,
)


def schema_retriever(state: AgentState) -> AgentState:
    question = state["question"]
    tables = retrieve_tables(question, top_k=4)
    scores = retrieve_tables_with_scores(question, top_k=4)

    log_entries = [f"[schema_retriever] retrieved: {tables}"]
    for s in scores:
        log_entries.append(
            f"  - {s['table_name']} (relevance={s['relevance']})"
        )

    return {
        "retrieved_tables": tables,
        "execution_log": state.get("execution_log", []) + log_entries,
    }
