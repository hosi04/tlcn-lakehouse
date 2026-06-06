import re
from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.agent.prompts import COLUMN_PRUNE_PROMPT
from src.chatbot.backend.llm_connector import get_llm


def _format_full_schema(full_schemas: dict) -> str:
    lines = []
    for table_name, meta in full_schemas.items():
        lines.append(f"\n-- {table_name}")
        lines.append(f"-- {meta.get('description', '')}")
        lines.append(f"{table_name} (")
        for col in meta.get("columns", []):
            col_desc = col.get("description", "")
            col_type = col.get("type", "")
            key_flag = " [PK]" if col.get("is_key") else ""
            fk_flag = f" [FK→{col.get('references', '')}]" if col.get("is_fk") else ""
            lines.append(
                f"    {col['name']} {col_type}{key_flag}{fk_flag},  -- {col_desc}"
            )
        lines.append(")")

        join_hints = meta.get("join_hints", [])
        if join_hints:
            lines.append("[JOINS]")
            for j in join_hints:
                lines.append(f"  {j}")

    return "\n".join(lines)


def _count_columns(pruned_schema: str) -> int:
    return len(re.findall(r"^\s+\w+\s+\w+", pruned_schema, re.MULTILINE))


def column_pruner(state: AgentState) -> AgentState:
    question = state.get("contextualized_question", state["question"])
    full_schemas = state.get("full_schemas", {})

    if not full_schemas:
        return {
            "pruned_schema": "",
            "columns_pruned_count": 0,
            "execution_log": state.get("execution_log", [])
            + ["[column_pruner] Không có schema để prune"],
        }

    full_schema_text = _format_full_schema(full_schemas)

    total_cols_before = sum(
        len(m.get("columns", [])) for m in full_schemas.values()
    )

    prompt = COLUMN_PRUNE_PROMPT.format(
        question=question,
        full_schema=full_schema_text,
    )
    pruned = get_llm().invoke(prompt).content.strip()

    cols_after = _count_columns(pruned)
    pruned_count = max(0, total_cols_before - cols_after)

    return {
        "pruned_schema": pruned,
        "columns_pruned_count": pruned_count,
        "schemas_used": list(full_schemas.keys()),
        "execution_log": state.get("execution_log", []) + [
            f"[column_pruner] Trước: {total_cols_before} cột → Sau: {cols_after} cột"
            f" (pruned {pruned_count})"
        ],
    }
