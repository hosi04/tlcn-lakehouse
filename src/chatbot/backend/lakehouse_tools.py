from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

_METADATA_PATH = Path(__file__).parent / "mcp_server" / "schema_metadata.yaml"
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_READ_ONLY_START_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
_FORBIDDEN_SQL_RE = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|merge|call|grant|revoke)\b",
    re.IGNORECASE,
)


def _get_trino_conn():
    from trino.dbapi import connect

    return connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user=os.getenv("TRINO_USER", "admin"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "gold"),
    )


def _close_quietly(resource: Any) -> None:
    close = getattr(resource, "close", None)
    if callable(close):
        close()


@lru_cache(maxsize=1)
def load_schema_metadata() -> dict:
    with open(_METADATA_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def normalize_table_name(table_name: str) -> str:
    name = table_name.strip()
    if "." not in name:
        name = f"iceberg.gold.{name}"
    parts = name.split(".")
    if len(parts) != 3 or not all(_IDENTIFIER_RE.match(part) for part in parts):
        raise ValueError(f"Tên bảng không hợp lệ: {table_name}")
    return ".".join(parts)


def _split_table_name(table_name: str) -> tuple[str, str, str]:
    normalized = normalize_table_name(table_name)
    catalog, schema_name, tbl = normalized.split(".")
    return catalog, schema_name, tbl


def _trino_fetchall(sql: str) -> list[dict[str, Any]]:
    conn = _get_trino_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows]
    finally:
        _close_quietly(cur)
        _close_quietly(conn)


def _yaml_columns_as_rows(columns_yaml: dict) -> list[dict[str, Any]]:
    rows = []
    for position, (col_name, col_meta) in enumerate(columns_yaml.items(), start=1):
        rows.append({
            "column_name": col_name,
            "data_type": col_meta.get("type", "UNKNOWN"),
            "ordinal_position": position,
        })
    return rows


def _merge_columns(
    col_rows: list[dict[str, Any]],
    columns_yaml: dict,
) -> list[dict[str, Any]]:
    merged = []
    for row in col_rows:
        col_name = row["column_name"]
        yaml_col = columns_yaml.get(col_name, {})
        merged.append({
            "name": col_name,
            "type": row.get("data_type", "UNKNOWN"),
            "position": row.get("ordinal_position"),
            "description": yaml_col.get("description", ""),
            "is_key": yaml_col.get("is_key", False),
            "is_fk": yaml_col.get("is_fk", False),
            "references": yaml_col.get("references"),
        })
    return merged


def list_tables(schema: str = "gold") -> list[str]:
    if not _IDENTIFIER_RE.match(schema):
        raise ValueError(f"Schema không hợp lệ: {schema}")
    rows = _trino_fetchall(
        "SELECT table_name FROM iceberg.information_schema.tables "
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    )
    return [f"iceberg.{schema}.{row['table_name']}" for row in rows]


def get_table_metadata(table_name: str) -> dict[str, Any]:
    normalized = normalize_table_name(table_name)
    catalog, schema_name, tbl = _split_table_name(normalized)

    yaml_data = load_schema_metadata()
    table_yaml = yaml_data.get("tables", {}).get(normalized, {})
    columns_yaml = table_yaml.get("columns", {})

    metadata_source = "trino"
    try:
        col_rows = _trino_fetchall(
            "SELECT column_name, data_type, ordinal_position "
            f"FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{schema_name}' AND table_name = '{tbl}' "
            "ORDER BY ordinal_position"
        )
    except Exception:
        col_rows = []
        metadata_source = "yaml_fallback"

    if not col_rows and columns_yaml:
        col_rows = _yaml_columns_as_rows(columns_yaml)
        metadata_source = "yaml_fallback"

    return {
        "table_name": normalized,
        "description": table_yaml.get("description", ""),
        "type": table_yaml.get("type", ""),
        "business_domain": table_yaml.get("business_domain", []),
        "join_hints": table_yaml.get("join_hints", []),
        "columns": _merge_columns(col_rows, columns_yaml),
        "metadata_source": metadata_source,
    }


def normalize_sql(sql: str) -> str:
    normalized = sql.strip().rstrip(";").strip()
    if ";" in normalized:
        raise ValueError("Chỉ cho phép một câu SQL duy nhất.")
    return normalized


def guard_read_only_sql(sql: str) -> str:
    normalized = normalize_sql(sql)
    if not _READ_ONLY_START_RE.match(normalized):
        raise ValueError("Chỉ cho phép truy vấn SELECT hoặc WITH ... SELECT.")
    if _FORBIDDEN_SQL_RE.search(normalized):
        raise ValueError("SQL chứa từ khóa thay đổi dữ liệu hoặc schema.")
    return normalized


def validate_sql(sql: str) -> dict[str, Any]:
    try:
        guarded = guard_read_only_sql(sql)
        conn = _get_trino_conn()
        cur = conn.cursor()
        try:
            cur.execute(f"EXPLAIN {guarded}")
            cur.fetchall()
            return {"valid": True, "sql": guarded, "error": None}
        finally:
            _close_quietly(cur)
            _close_quietly(conn)
    except Exception as exc:
        return {"valid": False, "sql": sql, "error": str(exc)}


def execute_sql(sql: str, max_rows: int = 1000) -> dict[str, Any]:
    validation = validate_sql(sql)
    if not validation["valid"]:
        return {
            "success": False,
            "error": validation["error"],
            "columns": [],
            "rows": [],
            "row_count": 0,
            "sql": sql,
        }

    guarded = validation["sql"]
    conn = _get_trino_conn()
    cur = conn.cursor()
    try:
        cur.execute(guarded)
        rows = cur.fetchmany(max_rows + 1)
        columns = [desc[0] for desc in cur.description]
        clipped_rows = rows[:max_rows]
        records = [dict(zip(columns, row)) for row in clipped_rows]
        return {
            "success": True,
            "error": None,
            "columns": columns,
            "rows": records,
            "row_count": len(records),
            "truncated": len(rows) > max_rows,
            "sql": guarded,
        }
    except Exception as exc:
        return {
            "success": False,
            "error": str(exc),
            "columns": [],
            "rows": [],
            "row_count": 0,
            "sql": guarded,
        }
    finally:
        _close_quietly(cur)
        _close_quietly(conn)
