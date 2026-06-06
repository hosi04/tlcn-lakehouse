from fastapi import FastAPI
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

app = FastAPI(title="Lakehouse NL2SQL Chatbot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_warmup():
    """Pre-warm LLM, embedding model và compile graph khi server khởi động."""
    print("[Startup] Pre-warming LLM...")
    get_llm()
    print("[Startup] Pre-warming embedding model & ChromaDB...")
    index_stats = ensure_index_ready()
    print(
        "[Startup] Index ready — "
        f"{index_stats['schema_count']} schemas, "
        f"{index_stats['sql_sample_count']} SQL samples"
    )
    print("[Startup] Pre-warming reranker model...")
    reranker_stats = warmup_reranker()
    print(f"[Startup] Reranker ready — {reranker_stats['model']}")
    print("[Startup] Compiling LangGraph agent graph...")
    get_graph()
    print("[Startup] ✅ Tất cả đã sẵn sàng — server phản hồi nhanh từ request đầu tiên!")


class QueryBody(BaseModel):
    query: str
    session_id: str = "default"


@app.post("/chat")
def chat(body: QueryBody):
    try:
        chat_history = get_recent_messages(body.session_id, max_turns=10)
        state = run_agent(body.query, chat_history=chat_history)
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

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "sql": None,
            "columns": [],
            "rows": [],
            "error_msg": str(e),
            "schemas_used": [],
            "columns_pruned": 0,
            "execution_log": [],
        }


@app.post("/clear-history")
def clear_chat_history(session_id: str = "default"):
    clear_history(session_id)
    return {"success": True, "message": f"History cleared for session {session_id}"}


@app.post("/index-schema")
def index_schema(force_rebuild: bool = False):
    try:
        stats = ensure_index_ready(force_rebuild=force_rebuild)
        return {
            "success": True,
            "message": "Schema đã được index thành công",
            **stats,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


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
