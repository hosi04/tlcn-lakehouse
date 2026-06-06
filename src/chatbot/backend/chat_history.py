from langchain_core.chat_history import InMemoryChatMessageHistory
from langchain_core.messages import HumanMessage, AIMessage

_store: dict[str, InMemoryChatMessageHistory] = {}


def get_session_history(session_id: str) -> InMemoryChatMessageHistory:
    if session_id not in _store:
        _store[session_id] = InMemoryChatMessageHistory()
    return _store[session_id]


def add_user_message(session_id: str, content: str) -> None:
    history = get_session_history(session_id)
    history.add_message(HumanMessage(content=content))


def add_ai_message(session_id: str, content: str) -> None:
    history = get_session_history(session_id)
    history.add_message(AIMessage(content=content))


def get_recent_messages(session_id: str, max_turns: int = 10) -> list:
    history = get_session_history(session_id)
    messages = history.messages
    return messages[-(max_turns * 2):]


def clear_history(session_id: str) -> None:
    if session_id in _store:
        _store[session_id].clear()
