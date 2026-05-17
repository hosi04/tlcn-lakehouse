import os
from dotenv import load_dotenv
from langchain_ollama import ChatOllama

load_dotenv()

OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")

_llm_instance = None

def get_llm():
    global _llm_instance
    if _llm_instance is None:
        print(f"[LLM] Dùng Ollama — model: {OLLAMA_MODEL} @ {OLLAMA_BASE_URL}")
        _llm_instance = ChatOllama(
            model=OLLAMA_MODEL,
            base_url=OLLAMA_BASE_URL,
            temperature=0.0,
            num_predict=2048,
        )
    return _llm_instance