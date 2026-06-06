from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import INTENT_PROMPT
from src.chatbot.backend.llm_connector import get_llm

_llm = get_llm()


def intent_classifier(state: AgentState) -> AgentState:
    question = state.get("contextualized_question", state["question"])
    prompt = INTENT_PROMPT.format(question=question)
    response = _llm.invoke(prompt).content.strip()

    lines = [l.strip() for l in response.splitlines() if l.strip()]
    intent = lines[0].lower() if lines else "data_query"

    if intent not in ("data_query", "greeting", "out_of_scope"):
        intent = "data_query"

    update: AgentState = {
        "intent": intent,
        "execution_log": state.get("execution_log", [])
        + [f"[intent_classifier] intent={intent}"],
    }

    if intent != "data_query" and len(lines) > 1:
        update["direct_answer"] = "\n".join(lines[1:])
    elif intent != "data_query":
        update["direct_answer"] = (
            "Xin chào! Tôi là trợ lý phân tích dữ liệu Lakehouse."
            if intent == "greeting"
            else "Xin lỗi, tôi chỉ có thể trả lời các câu hỏi liên quan đến dữ liệu kinh doanh."
        )

    return update
