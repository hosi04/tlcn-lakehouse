import logging
from functools import lru_cache

from sentence_transformers import CrossEncoder

logger = logging.getLogger(__name__)

RERANKER_MODEL = "cross-encoder/ms-marco-MiniLM-L-6-v2"


@lru_cache(maxsize=1)
def _get_reranker() -> CrossEncoder:
    logger.info("[Reranker] Loading %s...", RERANKER_MODEL)
    model = CrossEncoder(RERANKER_MODEL)
    logger.info("[Reranker] Model loaded.")
    return model


def warmup_reranker() -> dict:
    _get_reranker()
    return {
        "model": RERANKER_MODEL,
        "loaded": True,
    }


def rerank_schemas(
    question: str,
    candidates: list[dict],
    top_k: int = 4,
) -> list[dict]:
    if not candidates:
        return []

    reranker = _get_reranker()

    pairs = [(question, c["document"]) for c in candidates]
    scores = reranker.predict(pairs)

    for candidate, score in zip(candidates, scores):
        candidate["rerank_score"] = float(score)

    candidates.sort(key=lambda x: x["rerank_score"], reverse=True)

    logger.info(
        "[Reranker] Reranked %d candidates → top %d",
        len(candidates), min(top_k, len(candidates))
    )
    for c in candidates[:top_k]:
        logger.info(
            "  - %s | cosine_dist=%.4f | rerank_score=%.4f",
            c.get("table_name", "?"),
            c.get("distance", -1),
            c.get("rerank_score", -1),
        )

    return candidates[:top_k]
