from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.retrieval.schema_indexer import get_collection
from src.chatbot.backend.retrieval.reranker import rerank_schemas


def _query_chromadb(collection, query: str, top_k: int = 4) -> list[dict]:
    if collection.count() == 0:
        return []

    results = collection.query(
        query_texts=[query],
        n_results=min(top_k, collection.count()),
        include=["metadatas", "distances", "documents"],
    )

    candidates = []
    if results and results["ids"]:
        for table_id, distance, doc in zip(
            results["ids"][0],
            results["distances"][0],
            results["documents"][0],
        ):
            candidates.append({
                "table_name": table_id,
                "distance": distance,
                "document": doc,
            })
    return candidates


def _merge_candidates(all_candidates: list[dict]) -> list[dict]:
    seen = {}
    for c in all_candidates:
        name = c["table_name"]
        if name not in seen or c["distance"] < seen[name]["distance"]:
            seen[name] = c
    return list(seen.values())


def schema_retriever(state: AgentState) -> AgentState:
    question = state.get("contextualized_question", state["question"])
    sub_queries = state.get("sub_queries", [])
    log = list(state.get("execution_log", []))

    collection = get_collection()

    all_queries = [question] + sub_queries
    all_candidates = []

    for query in all_queries:
        candidates = _query_chromadb(collection, query, top_k=4)
        all_candidates.extend(candidates)

    merged = _merge_candidates(all_candidates)

    log.append(
        f"[schema_retriever] Multi-query: {len(all_queries)} queries "
        f"→ {len(all_candidates)} raw → {len(merged)} unique candidates"
    )

    if not merged:
        tables = ["iceberg.gold.fact_order", "iceberg.gold.dim_date"]
        log.append(f"[schema_retriever] No candidates → fallback: {tables}")
        return {
            "retrieved_tables": tables,
            "retrieved_candidates": [],
            "execution_log": log,
        }

    reranked = rerank_schemas(question, merged, top_k=4)

    tables = [c["table_name"] for c in reranked if c.get("rerank_score", 0) > -5]
    if not tables:
        tables = [reranked[0]["table_name"]] if reranked else [
            "iceberg.gold.fact_order", "iceberg.gold.dim_date"
        ]

    for c in reranked:
        log.append(
            f"  - {c['table_name']} "
            f"(cosine={c['distance']:.4f}, rerank={c.get('rerank_score', 0):.4f})"
        )

    log.append(f"[schema_retriever] Final tables: {tables}")

    return {
        "retrieved_tables": tables,
        "retrieved_candidates": reranked,
        "execution_log": log,
    }
