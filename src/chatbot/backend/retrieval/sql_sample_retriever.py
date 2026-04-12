from __future__ import annotations

from src.chatbot.backend.retrieval.schema_indexer import get_sql_sample_collection

# Ngưỡng cosine distance — ví dụ quá xa thì bỏ (không liên quan)
_DISTANCE_THRESHOLD = 0.6


def retrieve_sql_examples(question: str, top_k: int = 3) -> list[dict]:
    """
    Tìm top-K SQL examples tương tự nhất với câu hỏi.

    Args:
        question: Câu hỏi tiếng Việt của người dùng
        top_k: Số examples tối đa cần lấy (mặc định 3)

    Returns:
        List of dicts: {question, sql, tables, similarity}
        Đã lọc theo distance threshold.
    """
    collection = get_sql_sample_collection()

    if collection.count() == 0:
        return []

    results = collection.query(
        query_texts=[question],
        n_results=min(top_k, collection.count()),
        include=["metadatas", "distances"],
    )

    examples = []
    if results and results["ids"]:
        for meta, distance in zip(
            results["metadatas"][0],
            results["distances"][0],
        ):
            # Chỉ lấy example đủ gần
            if distance <= _DISTANCE_THRESHOLD:
                examples.append({
                    "question": meta.get("question", ""),
                    "sql": meta.get("sql", ""),
                    "tables": meta.get("tables", ""),
                    "similarity": round(1 - distance, 4),
                })

    return examples


def format_examples_for_prompt(examples: list[dict]) -> str:
    """
    Format examples thành chuỗi để đưa vào prompt.
    Trả về chuỗi rỗng nếu không có examples.
    """
    if not examples:
        return ""

    lines = ["VÍ DỤ TƯƠNG TỰ (few-shot):"]
    for i, ex in enumerate(examples, 1):
        lines.append(f"\n-- Ví dụ {i}: {ex['question']}")
        lines.append(ex["sql"].strip())

    return "\n".join(lines)
