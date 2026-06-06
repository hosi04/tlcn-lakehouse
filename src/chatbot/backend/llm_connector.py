import os
import logging
from functools import lru_cache

from dotenv import load_dotenv
from langchain_ollama import ChatOllama

load_dotenv()
logger = logging.getLogger(__name__)

OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL")


@lru_cache(maxsize=1)
def get_llm() -> ChatOllama:
    logger.info("Initializing Ollama — model: %s @ %s", OLLAMA_MODEL, OLLAMA_BASE_URL)
    return ChatOllama(
        model=OLLAMA_MODEL,
        base_url=OLLAMA_BASE_URL,
        temperature=0.0,
        num_predict=2048,
    )