from __future__ import annotations

from src.chatbot.backend.retrieval.schema_indexer import get_collection


def retrieve_tables(question: str, top_k: int = 3) -> list[str]:
    collection = get_collection()

    if collection.count() == 0:
        return [
            "iceberg.gold.fact_order",
            "iceberg.gold.fact_order_item",
            "iceberg.gold.dim_date",
        ]

    results = collection.query(
        query_texts=[question],
        n_results=min(top_k, collection.count()),
        include=["metadatas", "distances", "documents"],
    )

    tables = []
    if results and results["ids"]:
        for table_id, distance in zip(
            results["ids"][0], results["distances"][0]
        ):
            if distance < 0.8:
                tables.append(table_id)

    if not tables:
        tables = ["iceberg.gold.fact_order", "iceberg.gold.dim_date"]

    return tables


def retrieve_tables_with_scores(
    question: str, top_k: int = 4
) -> list[dict]:
    collection = get_collection()

    if collection.count() == 0:
        return []

    results = collection.query(
        query_texts=[question],
        n_results=min(top_k, collection.count()),
        include=["metadatas", "distances", "documents"],
    )

    output = []
    if results and results["ids"]:
        for table_id, distance, doc in zip(
            results["ids"][0],
            results["distances"][0],
            results["documents"][0],
        ):
            output.append({
                "table_name": table_id,
                "distance": round(distance, 4),
                "relevance": round(1 - distance, 4),
                "preview": doc[:200] + "..." if len(doc) > 200 else doc,
            })

    return output
