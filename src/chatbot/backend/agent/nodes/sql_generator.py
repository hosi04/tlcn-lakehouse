from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import SQL_GEN_PROMPT, SQL_FIX_PROMPT
from src.chatbot.backend.llm_connector import get_llm
from src.chatbot.backend.retrieval.sql_sample_retriever import (
    retrieve_sql_examples,
    format_examples_for_prompt,
)

_llm = get_llm()


def _clean_sql(raw: str) -> str:
    """Loại bỏ markdown code fences và ký tự thừa"""
    sql = raw.strip()
    if sql.startswith("```"):
        lines = sql.splitlines()
        inner = lines[1:-1] if lines[-1].strip() == "```" else lines[1:]
        sql = "\n".join(inner).strip()
    sql = sql.rstrip(";").strip()
    return sql


def sql_generator(state: AgentState) -> AgentState:
    """Sinh SQL lần đầu hoặc tự sửa (self-correction) khi retry."""
    question = state["question"]
    pruned_schema = state.get("pruned_schema", "")
    retry_count = state.get("retry_count", 0)
    log = state.get("execution_log", [])

    if retry_count == 0:
        # ── Lần đầu: retrieve few-shot examples + generate ──────────────────
        examples = retrieve_sql_examples(question, top_k=3)
        few_shot_text = format_examples_for_prompt(examples)

        if examples:
            log = log + [
                f"[sql_generator] Few-shot: {len(examples)} examples "
                f"(similarity: {[e['similarity'] for e in examples]})"
            ]
        else:
            log = log + ["[sql_generator] Few-shot: không tìm được example phù hợp"]

        prompt = SQL_GEN_PROMPT.format(
            question=question,
            pruned_schema=pruned_schema,
            few_shot_examples=few_shot_text,
        )
    else:
        # ── Retry: fix SQL cũ dựa trên error message từ Trino ───────────────
        sql_error = state.get("sql_error", "")
        old_sql = state.get("sql", "")
        prompt = SQL_FIX_PROMPT.format(
            question=question,
            sql=old_sql,
            error=sql_error,
            pruned_schema=pruned_schema,
        )

    raw = _llm.invoke(prompt).content
    sql = _clean_sql(raw)

    return {
        "sql": sql,
        "execution_log": log + [
            f"[sql_generator] attempt={retry_count + 1}, sql_preview={sql[:80]}..."
        ],
    }
