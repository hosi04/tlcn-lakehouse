from __future__ import annotations
from typing import Any, Optional
from typing_extensions import TypedDict


class AgentState(TypedDict, total=False):
    # ── Input ──────────────────────────────────────────────────────────────
    question: str                       # Câu hỏi gốc của người dùng

    # ── Intent ─────────────────────────────────────────────────────────────
    intent: str                         # "data_query" | "greeting" | "out_of_scope"
    direct_answer: Optional[str]        # Câu trả lời trực tiếp (nếu không phải data query)

    # ── Schema Retrieval ───────────────────────────────────────────────────
    retrieved_tables: list[str]         # Tên bảng từ RAG (iceberg.gold.xxx)
    full_schemas: dict[str, Any]        # Metadata đầy đủ từ MCP get_table_metadata

    # ── Column Pruning ─────────────────────────────────────────────────────
    pruned_schema: str                  # Schema tối giản sau khi loại cột thừa
    columns_pruned_count: int           # Số cột bị prune

    # ── SQL Generation & Validation ────────────────────────────────────────
    sql: str                            # SQL được sinh
    sql_error: Optional[str]           # Error message từ Trino (nếu có lỗi)
    retry_count: int                    # Số lần đã retry (max 3)

    # ── Query Result ──────────────────────────────────────────────────────
    query_result: list[dict[str, Any]]  # Dữ liệu trả về từ Trino
    columns: list[str]                  # Tên các cột trong kết quả
    row_count: int                      # Số dòng kết quả

    # ── Visualization ─────────────────────────────────────────────────────
    chart_config: dict[str, Any]        # Cấu hình chart cho Plotly

    # ── Report ────────────────────────────────────────────────────────────
    report: str                         # Báo cáo phân tích tiếng Việt (Markdown)

    # ── Debug / Metadata ──────────────────────────────────────────────────
    schemas_used: list[str]             # Tên bảng thực sự dùng trong SQL
    execution_log: list[str]            # Log từng bước để debug
