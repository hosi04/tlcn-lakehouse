from __future__ import annotations

import logging
import os
import warnings
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from src.chatbot.backend.agent.graph import run_agent, get_graph
from src.chatbot.backend.retrieval.schema_indexer import (
    ensure_index_ready,
    get_collection,
    get_sql_sample_collection,
)
from src.chatbot.backend.retrieval.reranker import warmup_reranker
from src.chatbot.backend.llm_connector import get_llm
from src.chatbot.backend.chat_history import (
    get_recent_messages,
    add_user_message,
    add_ai_message,
    clear_history,
)

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
warnings.filterwarnings("ignore")

logging.getLogger("httpx").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)

logger = logging.getLogger(__name__)

ALLOWED_ORIGINS = [
    "http://localhost:8501",
    "http://127.0.0.1:8501",
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("[Startup] Pre-warming LLM...")
    get_llm()
    logger.info("[Startup] Pre-warming embedding model & ChromaDB...")
    index_stats = ensure_index_ready()
    logger.info(
        "[Startup] Index ready — %d schemas, %d SQL samples",
        index_stats["schema_count"],
        index_stats["sql_sample_count"],
    )
    logger.info("[Startup] Pre-warming reranker model...")
    reranker_stats = warmup_reranker()
    logger.info("[Startup] Reranker ready — %s", reranker_stats["model"])
    logger.info("[Startup] Compiling LangGraph agent graph...")
    app.state.graph = get_graph()
    logger.info("[Startup] All systems ready.")
    yield


app = FastAPI(title="Lakehouse NL2SQL Chatbot", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)


class QueryBody(BaseModel):
    query: str
    session_id: str = "default"


@app.post("/chat")
def chat(body: QueryBody):
    chat_history = get_recent_messages(body.session_id, max_turns=10)
    try:
        state = run_agent(body.query, chat_history=chat_history)
    except Exception:
        logger.exception("Agent pipeline failed for query: %s", body.query)
        raise HTTPException(status_code=500, detail="Agent pipeline failed.")

    add_user_message(body.session_id, body.query)

    intent = state.get("intent", "data_query")
    if intent != "data_query":
        ai_content = state.get("direct_answer", "")
        add_ai_message(body.session_id, ai_content)
        return {
            "success": True,
            "intent": intent,
            "direct_answer": ai_content,
            "sql": None,
            "columns": [],
            "rows": [],
            "schemas_used": [],
            "columns_pruned": 0,
            "execution_log": state.get("execution_log", []),
        }

    sql = state.get("sql", "")
    row_count = state.get("row_count", 0)
    analysis = state.get("analysis", "")
    ai_summary = analysis or (f"[SQL] {sql[:100]}... → {row_count} rows" if sql else "")
    if ai_summary:
        add_ai_message(body.session_id, ai_summary)

    return {
        "success": True,
        "intent": intent,
        "sql": sql,
        "columns": state.get("columns", []),
        "rows": state.get("query_result", []),
        "row_count": row_count,
        "schemas_used": state.get("schemas_used", []),
        "columns_pruned": state.get("columns_pruned_count", 0),
        "contextualized_question": state.get("contextualized_question", ""),
        "analysis": state.get("analysis", ""),
        "execution_log": state.get("execution_log", []),
    }


@app.post("/clear-history")
def clear_chat_history(session_id: str = "default"):
    clear_history(session_id)
    return {"success": True, "message": f"History cleared for session {session_id}"}


@app.post("/index-schema")
def index_schema(force_rebuild: bool = False):
    try:
        stats = ensure_index_ready(force_rebuild=force_rebuild)
    except Exception:
        logger.exception("Schema indexing failed")
        raise HTTPException(status_code=500, detail="Schema indexing failed.")

    return {
        "success": True,
        "message": "Schema đã được index thành công",
        **stats,
    }


@app.get("/health")
def health():
    schema_count = get_collection().count()
    sql_sample_count = get_sql_sample_collection().count()
    status = "ok" if schema_count and sql_sample_count else "index_not_ready"
    return {
        "status": status,
        "version": "3.1-advanced-rag-multi-agent",
        "schema_count": schema_count,
        "sql_sample_count": sql_sample_count,
    }
