# NL2SQL Agent: LangGraph + MCP + RAG + Visualization

## Bối cảnh & Vấn đề

**Hiện tại**: Schema hard-coded trong prompt → không scale, không có xử lý lỗi, không có visualization.

**Mục tiêu**: Xây dựng hệ thống NL2SQL chuẩn enterprise với:
- **MCP Server** — chuẩn hóa truy cập Trino metadata (không cần re-index thủ công)
- **LangGraph** — orchestrate multi-agent workflow dạng state machine
- **RAG** — tìm bảng liên quan (Table Retrieval)
- **SQL Column Pruning** — loại cột thừa trước khi sinh SQL
- **Self-Correction** — tự sửa SQL nếu lỗi (tối đa 3 lần)
- **Report + Visualization** — tổng hợp báo cáo tiếng Việt + tự chọn loại chart

---

## Kiến trúc tổng thể

```
User Question
      │
      ▼
┌─────────────────────────────────────────────────────────────────┐
│                        LANGGRAPH WORKFLOW                       │
│                                                                 │
│  [1] Intent Classifier                                          │
│       └─ Phân loại: data_query / greeting / out_of_scope        │
│       └─ Nếu out_of_scope → trả lời trực tiếp, STOP            │
│                    │                                            │
│  [2] Schema Retrieval (RAG via ChromaDB)                        │
│       └─ Embed câu hỏi → tìm Top-K bảng liên quan              │
│       └─ MCP Tool: get_table_metadata(table_name)               │
│                    │                                            │
│  [3] Column Pruner  ← UBER technique                            │
│       └─ LLM chọn chỉ các cột liên quan từ bảng đã retrieve     │
│       └─ Sinh "skinny schema" (schema tối giản)                 │
│                    │                                            │
│  [4] SQL Generator                                              │
│       └─ Prompt = Rules + Skinny Schema + Question              │
│       └─ LLM sinh Trino SQL                                     │
│                    │                                            │
│  [5] SQL Validator (+ Self-Correction loop, max 3 retries)      │
│       └─ MCP Tool: execute_sql(sql)                             │
│       └─ Nếu lỗi → quay lại [4] với error context              │
│                    │                                            │
│  [6] Visualization Selector                                     │
│       └─ LLM quyết định: line / bar / pie / table / kpi_card    │
│       └─ Trả về chart_config (type, x, y, title)               │
│                    │                                            │
│  [7] Report Generator                                           │
│       └─ LLM tổng hợp insights dạng báo cáo tiếng Việt         │
│       └─ Có số liệu cụ thể, xu hướng, nhận xét                 │
└─────────────────────────────────────────────────────────────────┘
      │
      ▼
  Response: { sql, data, chart_config, report_markdown }
```

---

## Về Metadata

> [!IMPORTANT]
> **Trino `information_schema`** chỉ có: tên bảng, tên cột, kiểu dữ liệu — **KHÔNG có mô tả cột**.
> Iceberg có hỗ trợ column comments nhưng phải set thủ công khi tạo bảng (`COMMENT ON COLUMN ...`).
>
> **Giải pháp**: Dùng file `schema_metadata.yaml` để augment — kết hợp thông tin structural từ Trino với mô tả ngữ nghĩa viết tay. MCP Server sẽ merge cả hai nguồn khi trả về metadata.

**Ví dụ `schema_metadata.yaml`**:
```yaml
iceberg.gold.fact_order:
  description: "Bảng fact tổng hợp theo đơn hàng, mỗi row là 1 đơn"
  business_domain: ["đơn hàng", "doanh thu", "thanh toán", "giao hàng"]
  columns:
    total_payment_value:
      description: "Tổng giá trị thanh toán của đơn hàng (bao gồm freight)"
    purchase_date_key:
      description: "FK sang dim_date, ngày đặt hàng ở định dạng YYYYMMDD"
    delivery_actual_days:
      description: "Số ngày giao hàng thực tế"
```
MCP Server sẽ `SHOW COLUMNS FROM table` qua Trino, rồi **merge** với YAML để trả về metadata đầy đủ.

---

## Proposed Changes

### Component 1: MCP Server (FastMCP) — NEW

> [!NOTE]
> MCP Server chạy độc lập, expose các tools dưới dạng chuẩn MCP. LangGraph Agent gọi qua `langchain-mcp-adapters`.

#### [NEW] `src/chatbot/backend/mcp_server/trino_mcp_server.py`

**Tools được expose:**

| Tool | Input | Output | Mô tả |
|------|-------|--------|-------|
| `list_tables` | `schema: str` | `list[str]` | Liệt kê tất cả bảng trong schema |
| `get_table_metadata` | `table_name: str` | `TableMetadata` | Metadata đầy đủ: columns + types + descriptions |
| `execute_sql` | `sql: str` | `QueryResult` | Chạy SQL trên Trino, trả về JSON rows |
| `validate_sql` | `sql: str` | `ValidationResult` | EXPLAIN SQL — check syntax không cần chạy thật |

```python
# FastMCP pattern
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("Trino Lakehouse")

@mcp.tool()
def get_table_metadata(table_name: str) -> dict:
    """Lấy metadata đầy đủ của bảng: columns, types, descriptions, join hints"""
    # 1. Query Trino information_schema
    # 2. Merge với schema_metadata.yaml
    # 3. Return merged metadata
```

#### [NEW] `src/chatbot/backend/mcp_server/schema_metadata.yaml`

File YAML chứa toàn bộ mô tả ngữ nghĩa cho từng bảng và cột.

---

### Component 2: Schema RAG (ChromaDB local) — NEW

#### [NEW] `src/chatbot/backend/retrieval/schema_indexer.py`

- Đọc metadata từ MCP `get_table_metadata` → tạo document cho **mỗi bảng**
- Embed bằng **`sentence-transformers/all-MiniLM-L6-v2`** (local, offline, free)
- Lưu vào **ChromaDB** persistent ở `schema_store/`
- Chỉ cần chạy **một lần**, hoặc chạy lại khi thêm bảng mới

**Document schema** (mỗi bảng = 1 document):
```
[TABLE] iceberg.gold.fact_order
[DOMAIN] đơn hàng, doanh thu, thanh toán, giao hàng
[DESC] Bảng fact tổng hợp theo đơn hàng
[COLUMNS] order_key, customer_key, purchase_date_key, total_payment_value, ...
[JOINS] JOIN dim_customer ON customer_key | JOIN dim_date ON purchase_date_key
```

#### [NEW] `src/chatbot/backend/retrieval/schema_retriever.py`

```python
def retrieve_tables(question: str, top_k: int = 3) -> list[str]:
    """Semantic search → trả về tên các bảng liên quan nhất"""
```

---

### Component 3: LangGraph Agent — NEW (thay thế sql_agent.py)

#### [NEW] `src/chatbot/backend/agent/graph.py`

Định nghĩa **StateGraph** với các nodes:

```python
class AgentState(TypedDict):
    question: str
    intent: str                    # "data_query" | "greeting" | "out_of_scope"
    retrieved_tables: list[str]    # từ RAG
    full_schemas: dict             # từ MCP get_table_metadata
    pruned_schema: str             # schema sau khi prune cột thừa
    sql: str                       # SQL được sinh ra
    sql_error: str                 # lỗi nếu có
    retry_count: int               # số lần retry
    query_result: list[dict]       # kết quả từ Trino
    chart_config: dict             # loại chart và cấu hình
    report: str                    # báo cáo tiếng Việt

# Graph edges (conditional)
workflow.add_conditional_edges("sql_validator", decide_retry_or_continue)
# - Nếu lỗi và retry < 3 → quay lại sql_generator
# - Nếu thành công → sang visualization_selector
```

**Nodes trong graph:**

| Node | Responsibility |
|------|---------------|
| `intent_classifier` | Phân loại ý định, STOP sớm nếu không phải data query |
| `schema_retriever` | Gọi ChromaDB → lấy top-K bảng |
| `metadata_fetcher` | Gọi MCP `get_table_metadata` cho từng bảng retrieve được |
| `column_pruner` | LLM loại bỏ cột không liên quan → skinny schema |
| `sql_generator` | LLM sinh Trino SQL từ skinny schema |
| `sql_validator` | MCP `execute_sql` → kiểm tra kết quả hoặc lấy error |
| `visualization_selector` | LLM chọn chart type dựa trên shape của data |
| `report_generator` | LLM tổng hợp báo cáo tiếng Việt |

#### [NEW] `src/chatbot/backend/agent/nodes/` — Mỗi node là 1 file riêng

```
nodes/
├── intent_classifier.py
├── schema_retriever.py
├── metadata_fetcher.py
├── column_pruner.py
├── sql_generator.py
├── sql_validator.py
├── visualization_selector.py
└── report_generator.py
```

#### [MODIFY] `src/chatbot/backend/agent/prompts.py` (thay thế `prompt.py`)

| Prompt | Nhiệm vụ |
|--------|----------|
| `INTENT_PROMPT` | Classify intent: data_query / greeting / out_of_scope |
| `COLUMN_PRUNE_PROMPT` | Chọn cột liên quan, loại bỏ cột thừa |
| `SQL_GEN_PROMPT` | Sinh SQL với skinny schema (dynamic, không hard-coded) |
| `SQL_FIX_PROMPT` | Sửa SQL dựa trên error message từ Trino |
| `VIZ_SELECT_PROMPT` | Quyết định chart type và config |
| `REPORT_PROMPT` | Tổng hợp báo cáo kiểu phân tích dữ liệu |

---

### Component 4: API Layer — MODIFY

#### [MODIFY] `src/chatbot/backend/app.py`

- Endpoint `/chat` trả về response đầy đủ:
```json
{
  "success": true,
  "sql": "SELECT ...",
  "columns": ["year", "month", "total_revenue"],
  "rows": [...],
  "chart_config": {
    "type": "line",
    "x": "month",
    "y": "total_revenue",
    "title": "Doanh thu theo tháng"
  },
  "report": "## Phân tích Doanh thu\n\nTrong năm 2018, tổng doanh thu đạt **X triệu**...",
  "schemas_used": ["fact_order", "dim_date"],
  "columns_pruned": 12
}
```
- Thêm endpoint `/index-schema` để trigger re-index ChromaDB khi cần

---

### Component 5: Frontend (Streamlit) — MODIFY

#### [MODIFY] `src/chatbot/frontend/ui.py`

Thêm 3 phần mới vào response display:

1. **📊 Visualization** — Render chart bằng **Plotly** dựa trên `chart_config`
   - Line chart → doanh thu theo thời gian
   - Bar chart → so sánh sản phẩm, seller
   - Pie chart → phân phối trạng thái, category
   - KPI Cards → số tổng hợp đơn lẻ
   - Table → dữ liệu thô khi không phù hợp chart

2. **📝 Báo cáo** — Render `report` Markdown với `st.markdown()`
   - Số liệu nổi bật
   - Xu hướng
   - Nhận xét phân tích

3. **🔍 Debug Panel** (có thể toggle ẩn/hiện)
   - SQL được sinh ra
   - Bảng sử dụng
   - Số cột sau pruning

---

## Cấu trúc thư mục sau refactor

```
src/chatbot/
├── backend/
│   ├── app.py                          # FastAPI (modified)
│   ├── gemini_llm.py                   # Không đổi
│   ├── trino_connector.py              # Không đổi
│   │
│   ├── mcp_server/                     # NEW
│   │   ├── trino_mcp_server.py         # FastMCP server
│   │   └── schema_metadata.yaml        # Mô tả ngữ nghĩa thủ công
│   │
│   ├── retrieval/                      # NEW
│   │   ├── schema_indexer.py           # Index schema → ChromaDB
│   │   └── schema_retriever.py         # Semantic search
│   │
│   ├── agent/                          # NEW (thay sql_agent.py)
│   │   ├── graph.py                    # LangGraph StateGraph
│   │   ├── state.py                    # AgentState TypedDict
│   │   ├── prompts.py                  # Tất cả prompt templates
│   │   └── nodes/
│   │       ├── intent_classifier.py
│   │       ├── schema_retriever.py
│   │       ├── metadata_fetcher.py
│   │       ├── column_pruner.py
│   │       ├── sql_generator.py
│   │       ├── sql_validator.py
│   │       ├── visualization_selector.py
│   │       └── report_generator.py
│   │
│   └── schema_store/                   # AUTO-GENERATED (ChromaDB data)
│
└── frontend/
    └── ui.py                           # Streamlit (modified, + Plotly)
```

---

## Dependencies mới

```
# requirements.txt additions
langgraph                      # LangGraph workflow
langchain-mcp-adapters         # MCP ↔ LangChain bridge
mcp[cli]                       # FastMCP server
chromadb                       # Local vector store
sentence-transformers          # Local embedding (all-MiniLM-L6-v2)
plotly                         # Visualization
pyyaml                         # Đọc schema_metadata.yaml
```

---

## So sánh Before vs After

| Tiêu chí | Before | After |
|----------|--------|-------|
| Schema trong prompt | Hard-coded toàn bộ 6 bảng (~3000 tokens) | Chỉ cột liên quan (~400–700 tokens) |
| Column Selection | Không có | Column Pruning (Uber technique) |
| Xử lý lỗi SQL | Không có | Self-correction tối đa 3 lần |
| Metadata source | Thủ công trong code | Trino info_schema + YAML augmentation |
| Output | SQL + raw table | SQL + Plotly chart + Báo cáo Markdown |
| Orchestration | Linear chain | LangGraph state machine |
| Tool access | Direct function call | Chuẩn MCP (swappable) |
| Embedding | Không có | sentence-transformers local |
| LLM | Gemini API | Gemini API (→ self-hosted sau) |

---

## Thứ tự implementation (recommended)

```
Phase 1 (Foundation):
  [1] schema_metadata.yaml — viết mô tả cho 6 bảng hiện có
  [2] trino_mcp_server.py — expose list_tables, get_table_metadata, execute_sql
  [3] schema_indexer.py + schema_retriever.py — ChromaDB + sentence-transformers

Phase 2 (Agent):
  [4] state.py + graph.py — LangGraph skeleton
  [5] Implement từng node theo thứ tự graph
  [6] prompts.py — viết đủ 6 prompt templates

Phase 3 (Output):
  [7] app.py — cập nhật response schema
  [8] ui.py — thêm Plotly + Report render
```

---

## Verification Plan

### Automated
- Unit test: `retrieve_tables("doanh thu theo tháng")` → phải trả về `fact_order`, `dim_date`
- Unit test: `column_pruner` → schema sau prune không chứa cột `product_weight_g` khi hỏi về doanh thu
- Integration test: End-to-end 5 câu hỏi mẫu → SQL valid + data trả về
- Test self-correction: Inject SQL lỗi → kiểm tra agent tự sửa

### Manual
- Kiểm tra Streamlit UI: chart render đúng loại theo câu hỏi
- Kiểm tra báo cáo: có số liệu, xu hướng, tiếng Việt tự nhiên
- So sánh token usage: before vs after với cùng câu hỏi
