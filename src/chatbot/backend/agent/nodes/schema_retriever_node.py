from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.retrieval.schema_retriever import retrieve_tables_with_scores


def schema_retriever(state: AgentState) -> AgentState:
    question = state["question"]
    # Gọi ChromaDB 1 lần duy nhất thay vì 2 lần
    scores = retrieve_tables_with_scores(question, top_k=4)

    tables = [s["table_name"] for s in scores if s["distance"] < 0.8]
    if not tables:
        tables = ["iceberg.gold.fact_order", "iceberg.gold.dim_date"]

    log_entries = [f"[schema_retriever] retrieved: {tables}"]
    for s in scores:
        log_entries.append(
            f"  - {s['table_name']} (relevance={s['relevance']})"
        )

    return {
        "retrieved_tables": tables,
        "execution_log": state.get("execution_log", []) + log_entries,
    }
