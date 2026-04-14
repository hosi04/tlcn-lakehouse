import json
from pathlib import Path
import yaml
import os

from src.chatbot.backend.agent.state import AgentState
from src.chatbot.backend.trino_connector import trino_query

_METADATA_PATH = (
    Path(__file__).parent.parent.parent
    / "mcp_server"
    / "schema_metadata.yaml"
)


def _get_trino_columns(table_name: str) -> list[dict]:
    parts = table_name.split(".")
    if len(parts) == 3:
        catalog, schema_name, tbl = parts
    else:
        catalog, schema_name, tbl = "iceberg", "gold", parts[-1]

    try:
        df = trino_query(
            f"SELECT column_name, data_type, ordinal_position "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{tbl}' "
            f"ORDER BY ordinal_position"
        )
        return df.to_dict(orient="records")
    except Exception:
        return []


def _load_yaml() -> dict:
    with open(_METADATA_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def metadata_fetcher(state: AgentState) -> AgentState:
    retrieved_tables = state.get("retrieved_tables", [])
    yaml_data = _load_yaml()
    yaml_tables = yaml_data.get("tables", {})

    full_schemas = {}
    log_entries = []

    for table_name in retrieved_tables:
        # Normalize
        if "." not in table_name:
            table_name = f"iceberg.gold.{table_name}"

        table_yaml = yaml_tables.get(table_name, {})
        col_rows = _get_trino_columns(table_name)
        columns_yaml = table_yaml.get("columns", {})

        columns_merged = []
        for row in col_rows:
            col_name = row["column_name"]
            yaml_col = columns_yaml.get(col_name, {})
            columns_merged.append({
                "name": col_name,
                "type": row["data_type"],
                "description": yaml_col.get("description", ""),
                "is_key": yaml_col.get("is_key", False),
                "is_fk": yaml_col.get("is_fk", False),
                "references": yaml_col.get("references"),
            })

        full_schemas[table_name] = {
            "table_name": table_name,
            "description": table_yaml.get("description", ""),
            "type": table_yaml.get("type", ""),
            "business_domain": table_yaml.get("business_domain", []),
            "join_hints": table_yaml.get("join_hints", []),
            "columns": columns_merged,
        }
        log_entries.append(
            f"[metadata_fetcher] {table_name}: {len(columns_merged)} columns"
        )

    return {
        "full_schemas": full_schemas,
        "execution_log": state.get("execution_log", []) + log_entries,
    }
