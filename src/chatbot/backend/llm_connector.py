import os
from dotenv import load_dotenv
from langchain_ollama import ChatOllama

load_dotenv()

OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")

def get_llm():
    print(f"[LLM] Dùng Ollama — model: {OLLAMA_MODEL} @ {OLLAMA_BASE_URL}")
    llm = ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0.0,
        num_predict=2048,
    )
    return llm