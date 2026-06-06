from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.lakehouse_tools import get_table_metadata, normalize_table_name


def metadata_fetcher(state: AgentState) -> AgentState:
    retrieved_tables = state.get("retrieved_tables", [])

    full_schemas = {}
    log_entries = []

    for table_name in retrieved_tables:
        table_name = normalize_table_name(table_name)
        table_meta = get_table_metadata(table_name)
        full_schemas[table_name] = table_meta
        log_entries.append(
            f"[metadata_fetcher] {table_name}: {len(table_meta.get('columns', []))} columns "
            f"({table_meta.get('metadata_source', 'unknown')})"
        )

    return {
        "full_schemas": full_schemas,
        "execution_log": state.get("execution_log", []) + log_entries,
    }
