from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.chatbot.backend.agent.graph import run_agent, get_graph
from src.chatbot.backend.retrieval.schema_indexer import build_index, get_collection
from src.chatbot.backend.llm_connector import get_llm

app = FastAPI(title="Lakehouse NL2SQL Chatbot — Agent + RAG")

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
    get_llm()  # khởi tạo ChatOllama instance vào cache
    print("[Startup] Pre-warming embedding model & ChromaDB...")
    get_collection()  # khởi tạo SentenceTransformer + ChromaDB client vào cache
    print("[Startup] Compiling LangGraph agent graph...")
    get_graph()  # compile graph 1 lần duy nhất
    print("[Startup] ✅ Tất cả đã sẵn sàng — server phản hồi nhanh từ request đầu tiên!")


class QueryBody(BaseModel):
    query: str


@app.post("/chat")
def chat(body: QueryBody):
    try:
        state = run_agent(body.query)

        intent = state.get("intent", "data_query")
        if intent != "data_query":
            return {
                "success": True,
                "intent": intent,
                "direct_answer": state.get("direct_answer", ""),
                "sql": None,
                "columns": [],
                "rows": [],
                "schemas_used": [],
                "columns_pruned": 0,
                "execution_log": state.get("execution_log", []),
            }

        return {
            "success": True,
            "intent": intent,
            "sql": state.get("sql", ""),
            "columns": state.get("columns", []),
            "rows": state.get("query_result", []),
            "row_count": state.get("row_count", 0),
            "schemas_used": state.get("schemas_used", []),
            "columns_pruned": state.get("columns_pruned_count", 0),
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


@app.post("/index-schema")
def index_schema(force_rebuild: bool = False):
    try:
        build_index(force_rebuild=force_rebuild)
        return {"success": True, "message": "Schema đã được index thành công"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/health")
def health():
    return {"status": "ok", "version": "2.0-agent-rag"}
