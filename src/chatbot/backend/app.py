from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from src.chatbot.backend.sql_agent import chatbot_sql

app = FastAPI(title="Lakehouse SQL Chatbot")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryBody(BaseModel):
    query: str

@app.post("/chat")
def chat(query: QueryBody):
    try:
        result = chatbot_sql(query.query)
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/health")
def health():
    return {"status": "ok"}
