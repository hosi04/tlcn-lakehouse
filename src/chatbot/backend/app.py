from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from src.chatbot.backend.agent.graph import run_agent
from src.chatbot.backend.retrieval.schema_indexer import build_index

app = FastAPI(title="Lakehouse NL2SQL Chatbot — Agent + RAG")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


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
                "chart_config": None,
                "report": state.get("direct_answer", ""),
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
            "chart_config": state.get("chart_config", {}),
            "report": state.get("report", ""),
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
            "chart_config": None,
            "report": f"Lỗi hệ thống: {str(e)}",
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
