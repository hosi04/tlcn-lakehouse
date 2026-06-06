from typing import Any

from mcp.server.fastmcp import FastMCP
from src.chatbot.backend.lakehouse_tools import (
    execute_sql as run_execute_sql,
    get_table_metadata as load_table_metadata,
    list_tables as load_tables,
    validate_sql as run_validate_sql,
)

mcp = FastMCP(
    name="trino-lakehouse",
    instructions=(
        "MCP Server cung cấp quyền truy cập vào Trino Lakehouse. "
        "Dùng các tools để lấy metadata bảng, thực thi SQL, và liệt kê schema."
    ),
)

@mcp.tool()
def list_tables(schema: str = "gold") -> list[str]:
    return load_tables(schema=schema)


@mcp.tool()
def get_table_metadata(table_name: str) -> dict[str, Any]:
    return load_table_metadata(table_name)


@mcp.tool()
def execute_sql(sql: str) -> dict[str, Any]:
    return run_execute_sql(sql)


@mcp.tool()
def validate_sql(sql: str) -> dict[str, Any]:
    return run_validate_sql(sql)


if __name__ == "__main__":
    mcp.run(transport="stdio")
