import os
import json
import yaml
import pandas as pd
from pathlib import Path
from typing import Any

from mcp.server.fastmcp import FastMCP
from trino.dbapi import connect
from dotenv import load_dotenv

load_dotenv()

mcp = FastMCP(
    name="trino-lakehouse",
    instructions=(
        "MCP Server cung cấp quyền truy cập vào Trino Lakehouse. "
        "Dùng các tools để lấy metadata bảng, thực thi SQL, và liệt kê schema."
    ),
)

_METADATA_PATH = Path(__file__).parent / "schema_metadata.yaml"

def _load_yaml_metadata() -> dict:
    with open(_METADATA_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def _get_trino_conn():
    return connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", 8080)),
        user=os.getenv("TRINO_USER", "admin"),
        catalog=os.getenv("TRINO_CATALOG", "iceberg"),
        schema=os.getenv("TRINO_SCHEMA", "gold"),
    )


def _trino_fetchall(sql: str) -> list[dict]:
    conn = _get_trino_conn()
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in rows]


@mcp.tool()
def list_tables(schema: str = "gold") -> list[str]:
    rows = _trino_fetchall(
        f"SELECT table_name FROM iceberg.information_schema.tables "
        f"WHERE table_schema = '{schema}' ORDER BY table_name"
    )
    return [f"iceberg.{schema}.{r['table_name']}" for r in rows]


@mcp.tool()
def get_table_metadata(table_name: str) -> dict[str, Any]:
    if "." not in table_name:
        table_name = f"iceberg.gold.{table_name}"
    parts = table_name.split(".")
    catalog, schema_name, tbl = parts[0], parts[1], parts[2]
    col_rows = _trino_fetchall(
        f"SELECT column_name, data_type, ordinal_position "
        f"FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema_name}' AND table_name = '{tbl}' "
        f"ORDER BY ordinal_position"
    )

    yaml_meta = _load_yaml_metadata()
    table_yaml = yaml_meta.get("tables", {}).get(table_name, {})
    columns_yaml = table_yaml.get("columns", {})
    columns_merged = []
    for row in col_rows:
        col_name = row["column_name"]
        yaml_col = columns_yaml.get(col_name, {})
        columns_merged.append({
            "name": col_name,
            "type": row["data_type"],
            "position": row["ordinal_position"],
            "description": yaml_col.get("description", ""),
            "is_key": yaml_col.get("is_key", False),
            "is_fk": yaml_col.get("is_fk", False),
            "references": yaml_col.get("references", None),
        })

    return {
        "table_name": table_name,
        "description": table_yaml.get("description", ""),
        "type": table_yaml.get("type", ""),
        "business_domain": table_yaml.get("business_domain", []),
        "join_hints": table_yaml.get("join_hints", []),
        "columns": columns_merged,
    }


@mcp.tool()
def execute_sql(sql: str) -> dict[str, Any]:
    normalized = sql.strip().lower()
    if not normalized.startswith("select"):
        return {
            "success": False,
            "error": "Chỉ cho phép câu lệnh SELECT.",
            "columns": [],
            "rows": [],
            "row_count": 0,
        }

    try:
        conn = _get_trino_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        records = [dict(zip(columns, row)) for row in rows]
        return {
            "success": True,
            "columns": columns,
            "rows": records,
            "row_count": len(records),
            "error": None,
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "columns": [],
            "rows": [],
            "row_count": 0,
        }


@mcp.tool()
def validate_sql(sql: str) -> dict[str, Any]:
    try:
        conn = _get_trino_conn()
        cur = conn.cursor()
        cur.execute(f"EXPLAIN {sql}")
        cur.fetchall()
        return {"valid": True, "error": None}
    except Exception as e:
        return {"valid": False, "error": str(e)}


if __name__ == "__main__":
    mcp.run(transport="stdio")
