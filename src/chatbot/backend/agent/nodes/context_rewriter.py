from langchain_core.output_parsers import StrOutputParser

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import CONTEXTUALIZE_PROMPT
from src.chatbot.backend.llm_connector import get_llm

_chain = None


def _get_chain():
    global _chain
    if _chain is None:
        _chain = CONTEXTUALIZE_PROMPT | get_llm() | StrOutputParser()
    return _chain


def context_rewriter(state: AgentState) -> AgentState:
    question = state["question"]
    chat_history = state.get("chat_history", [])
    log = state.get("execution_log", [])

    if not chat_history:
        return {
            "contextualized_question": question,
            "execution_log": log + [
                f"[context_rewriter] No history → keep original: '{question}'"
            ],
        }

    chain = _get_chain()
    rewritten = chain.invoke({
        "chat_history": chat_history,
        "input": question,
    }).strip()

    changed = rewritten.lower() != question.lower()
    return {
        "contextualized_question": rewritten,
        "execution_log": log + [
            f"[context_rewriter] {'REWRITTEN' if changed else 'UNCHANGED'}: "
            f"'{question}' → '{rewritten}'"
        ],
    }
