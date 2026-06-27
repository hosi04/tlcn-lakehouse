from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import yaml
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

_RULES_PATH = Path(__file__).resolve().parent.parent / "business_rules.yaml"


@lru_cache(maxsize=1)
def load_business_rules() -> dict:
    with open(_RULES_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _build_schema_rules_text() -> str:
    rules = load_business_rules()
    lines = []
    for entry in rules.get("schema_rules", []):
        table = entry.get("table", "")
        for note in entry.get("notes", []):
            lines.append(f"- {table}: {note}")
    return "\n".join(lines)


def _build_sql_conventions_text() -> str:
    rules = load_business_rules()
    conv = rules.get("sql_conventions", {})
    lines = [
        f"- Luôn dùng tên bảng đầy đủ: {conv.get('table_prefix', 'iceberg.gold.')}<table_name>",
    ]
    aliases = conv.get("common_aliases", {})
    if aliases:
        alias_strs = [f"{k}: {v}" for k, v in aliases.items()]
        lines.append(f"- Alias phổ biến khi JOIN: {', '.join(alias_strs)}")
    join_strategy = conv.get("join_strategy", "")
    if join_strategy:
        lines.append(f"- {join_strategy}")
    date_join = conv.get("date_join", "")
    if date_join:
        lines.append(f"- {date_join}")
    return "\n".join(lines)


CONTEXTUALIZE_PROMPT = ChatPromptTemplate.from_messages([
    ("system",
     "Bạn là trợ lý phân tích dữ liệu. Nhiệm vụ: dựa trên lịch sử hội thoại "
     "và câu hỏi mới, viết lại câu hỏi thành một câu ĐỘC LẬP, HOÀN CHỈNH "
     "bằng tiếng Việt.\n\n"
     "QUY TẮC BẮT BUỘC:\n"
     "- Nếu câu hỏi mới tham chiếu đến ngữ cảnh trước (\"cái đó\", \"thêm\", "
     "\"theo tháng nữa\", \"của năm 2017\"), hãy thay thế bằng nội dung cụ thể.\n"
     "- Nếu câu hỏi đã rõ ràng, giữ nguyên.\n"
     "- TUYỆT ĐỐI KHÔNG TRẢ LỜI CÂU HỎI HAY TÓM TẮT DỮ LIỆU!!! CHỈ TRẢ VỀ DUY NHẤT 1 CÂU HỎI ĐÃ VIẾT LẠI."),
    MessagesPlaceholder("chat_history"),
    ("human", 
     "LỊCH SỬ HỘI THOẠI BÊN TRÊN LÀ ĐỂ THAM KHẢO NGỮ CẢNH.\n\n"
     "CÂU HỎI MỚI CỦA NGƯỜI DÙNG: {input}\n\n"
     "NHIỆM VỤ BẮT BUỘC: Hãy viết lại CÂU HỎI MỚI thành một câu hỏi HOÀN CHỈNH, ĐỘC LẬP. "
     "TUYỆT ĐỐI KHÔNG TRẢ LỜI CÂU HỎI HOẶC TẠO BÁO CÁO!!! CHỈ TRẢ VỀ DUY NHẤT CÂU HỎI ĐÃ VIẾT LẠI (hoặc giữ nguyên nếu đã rõ ràng)."),
])


MULTI_QUERY_PROMPT = """\
Bạn là trợ lý tìm kiếm schema database cho hệ thống e-commerce Lakehouse.

Nhiệm vụ: Sinh ra 3 câu hỏi phụ KHÁC NHAU để tìm kiếm bảng dữ liệu liên quan.
Mỗi câu hỏi phụ nên nhìn vấn đề từ một góc khác:
1. Câu hỏi về DỮ LIỆU cần truy vấn (metric, dimension)
2. Câu hỏi về BẢNG/CỘT liên quan
3. Câu hỏi về MỐI QUAN HỆ giữa các bảng

Câu hỏi gốc: {question}

Trả về ĐÚNG 3 dòng, mỗi dòng 1 câu hỏi phụ (không đánh số, không giải thích):
"""



INTENT_PROMPT = """\
Bạn là bộ phân loại ý định cho chatbot phân tích dữ liệu Lakehouse.

Phân loại câu hỏi sau vào MỘT trong 4 nhóm:
- "what_if_simulation": Câu hỏi mô phỏng kịch bản tương lai, dự đoán doanh thu nếu đạt KPI nào đó (ví dụ: "Nếu tuần tới...", "Dự đoán doanh thu nếu active_customers đạt...", "Giả sử phòng marketing mang về...")
- "data_query": Câu hỏi yêu cầu truy vấn, phân tích, thống kê dữ liệu hiện có trong database (doanh thu, đơn hàng, sản phẩm, khách hàng, seller, thời gian, ...)
- "greeting": Chào hỏi, hỏi thăm, giới thiệu bản thân
- "out_of_scope": Câu hỏi không liên quan đến dữ liệu (thời tiết, code, văn học, ...)

Câu hỏi: {question}

Trả lời ĐÚNG MỘT từ: what_if_simulation | data_query | greeting | out_of_scope
Nếu là greeting hoặc out_of_scope, thêm dòng thứ 2 là câu trả lời ngắn gọn bằng tiếng Việt.

Ví dụ output:
what_if_simulation
---
data_query
---
greeting
Xin chào! Tôi là trợ lý phân tích dữ liệu Lakehouse. Hãy hỏi tôi về doanh thu, đơn hàng, sản phẩm nhé!
"""


WHAT_IF_PROMPT = """\
Bạn là chuyên gia MLOps mô phỏng kịch bản kinh doanh (What-If Simulation Engine).
Nhiệm vụ của bạn là trích xuất các tham số kịch bản quản trị từ câu hỏi của người dùng để nạp vào mô hình AI (LightGBM/Prophet/LSTM).

CÂU HỎI CỦA NGƯỜI DÙNG: {question}

Các tham số có thể trích xuất (nếu câu hỏi đề cập):
- order_count (số đơn hàng)
- active_customers_count (số khách hàng hoạt động)
- avg_order_value (giá trị đơn hàng trung bình)
- avg_delivery_days (số ngày giao hàng trung bình)
- late_delivery_rate (tỷ lệ giao hàng trễ, ví dụ: 0.05 cho 5%)
- credit_card_ratio (tỷ lệ thanh toán qua thẻ tín dụng)

TRẢ VỀ KẾT QUẢ DƯỚI DẠNG JSON THUẦN (chỉ trả về JSON, không giải thích, không markdown):
{{
    "order_count": <số hoặc null>,
    "active_customers_count": <số hoặc null>,
    "avg_order_value": <số hoặc null>,
    "avg_delivery_days": <số hoặc null>,
    "late_delivery_rate": <số hoặc null>,
    "credit_card_ratio": <số hoặc null>
}}
"""



COLUMN_PRUNE_PROMPT = """\
Bạn là chuyên gia tối ưu schema cho NL2SQL. Nhiệm vụ: loại bỏ các cột KHÔNG CẦN THIẾT để trả lời câu hỏi.

CÂU HỎI: {question}

SCHEMA ĐẦY ĐỦ CÁC BẢNG:
{full_schema}

YÊU CẦU:
1. Giữ lại: surrogate keys (_key) cho JOIN, foreign keys (FK), cột được nhắc đến trong câu hỏi, cột cần thiết cho GROUP BY / ORDER BY / WHERE / aggregation
2. Loại bỏ: cột đơn vị vật lý không cần thiết (weight, length, width, height), cột mô tả không liên quan, business keys (_id) trừ khi được yêu cầu cụ thể
3. Giữ NGUYÊN tên bảng và FORMAT FORMAT sau

OUTPUT FORMAT (chỉ trả về schema tối giản, không giải thích):
-- <table_name>
-- Mô tả: <mô tả ngắn>
<table_name> (
    <col1> <TYPE>,  -- <mô tả ngắn>
    <col2> <TYPE>,  -- <mô tả ngắn>
    ...
)
[JOINS: <join hints liên quan>]

"""


SQL_GEN_PROMPT = """\
Bạn là chuyên gia SQL cho hệ thống Lakehouse chạy **Trino** (Apache Trino).

QUY TẮC BẮT BUỘC:
1. Chỉ sinh câu lệnh SELECT hợp lệ cho Trino — KHÔNG sinh DELETE, DROP, UPDATE, INSERT.
2. Luôn dùng alias khi JOIN nhiều bảng
3. Không dùng SELECT * — chỉ select cột cần thiết
4. Đặt alias rõ ràng cho aggregation (SUM(...) AS total_revenue)
5. Trả về SQL THUẦN — không có giải thích, không có markdown code block
6. Không tự chế cột ngoài schema đã cung cấp.
7. Khi hỏi tỷ lệ/phần trăm, phải có phép chia numerator / denominator, thường dùng SUM(CASE WHEN ... THEN 1 ELSE 0 END) * 100.0 / COUNT(*).
8. Khi hỏi top N, thêm ORDER BY metric DESC/ASC phù hợp và LIMIT N.
9. Trong bảng dim_date, cột `year` và `month` là kiểu số nguyên (integer). Để lọc năm hoặc tháng, hãy so sánh trực tiếp (ví dụ: `d.year = 2018`). TUYỆT ĐỐI KHÔNG DÙNG hàm `year(d.year)` hoặc `year(d.date_key)`.

QUY TẮC SQL:
""" + _build_sql_conventions_text() + """

QUY TẮC ĐẶC THÙ SCHEMA NÀY (BẮT BUỘC):
""" + _build_schema_rules_text() + """

SCHEMA CHỈ CÓ CÁC CỘT LIÊN QUAN (đã được prune):
{pruned_schema}

{few_shot_examples}

CÂU HỎI: {question}

SQL:
"""


SQL_FIX_PROMPT = """\
SQL sau khi chạy trên Trino bị lỗi. Hãy phân tích lỗi và sửa lại SQL.

CÂU HỎI GỐC: {question}

SQL BỊ LỖI:
```sql
{sql}
```

LỖI TỪ TRINO:
{error}

SCHEMA HIỆN CÓ (đã prune):
{pruned_schema}

HƯỚNG DẪN:
- Nếu lỗi "Column not found": kiểm tra tên cột trong schema, có thể cột không tồn tại hoặc cần alias bảng
- Nếu lỗi "Table not found": dùng tên đầy đủ iceberg.gold.<table>
- Nếu lỗi type/cast: thêm CAST hoặc dùng đúng kiểu dữ liệu
- Nếu lỗi ambiguous column: thêm alias bảng trước tên cột
- Không cast date_key sang DATE; date_key là số YYYYMMDD. Muốn lọc thời gian hãy JOIN dim_date và dùng d.year/d.month/d.quarter/d.is_weekend.
- Trong bảng dim_date, cột `year` và `month` là kiểu số nguyên (integer). Để lọc năm hoặc tháng, hãy so sánh trực tiếp (ví dụ: `d.year = 2018`). TUYỆT ĐỐI KHÔNG DÙNG hàm `year(d.year)` hoặc `year(d.date_key)`.
- dim_date không có delivery_actual_days hoặc shipping_days; dùng fact_order.delivery_actual_days hoặc fact_order_item.shipping_days.
- dim_product không có product_name; dùng product_id hoặc product_category_name_english.
- dim_seller không có customer_key; không join trực tiếp dim_customer với dim_seller bằng key.

Trả về SQL ĐÃ SỬA, THUẦN (không giải thích, không markdown):
SQL:
"""


ANALYST_PROMPT = """\
Bạn là chuyên gia phân tích dữ liệu e-commerce cho hệ thống Lakehouse.

CÂU HỎI CỦA NGƯỜI DÙNG: {question}

SQL ĐÃ CHẠY:
```sql
{sql}
```

KẾT QUẢ ({row_count} dòng, cột: {columns}):
{result_summary}

NHIỆM VỤ — Phân tích kết quả và trả lời theo 3 phần:

📊 **Tóm tắt:** Mô tả ngắn gọn kết quả chính (2-3 câu)

🔍 **Insight:** Phát hiện xu hướng, bất thường hoặc pattern đáng chú ý từ dữ liệu (2-3 bullet points)

💡 **Gợi ý hành động:** Đề xuất 1-2 hành động cụ thể mà doanh nghiệp nên thực hiện dựa trên dữ liệu này

QUY TẮC BẮT BUỘC (TUYỆT ĐỐI TUÂN THỦ):
1. ĐỌC KỸ BẢNG KẾT QUẢ VÀ TUYỆT ĐỐI SỬ DỤNG MỤC `[THỐNG KÊ NHANH TỪ HỆ THỐNG (DÀNH CHO BÁO CÁO)]`: 
   - Phần Tóm tắt phải nêu chính xác con số Tổng (Sum) từ mục thống kê nhanh.
   - Phần Insight phải nêu chính xác Đỉnh cao nhất (Max) rơi vào dòng nào (tháng/năm nào) và Thấp nhất (Min) rơi vào dòng nào theo đúng số liệu trong mục thống kê nhanh.
2. TUYỆT ĐỐI KHÔNG TỰ SO SÁNH HOẶC KẾT LUẬN BỪA (Ví dụ: Tháng 4 là 1,132,933.95 lớn hơn Tháng 7 là 1,027,903.86, do đó đỉnh cao nhất là Tháng 4, KHÔNG ĐƯỢC KẾT LUẬN SAI THÀNH THÁNG 7!!!).
3. TUYỆT ĐỐI KHÔNG BỊA ĐẶT HOẶC KHÁI QUÁT HÓA TÊN SẢN PHẨM/DANH MỤC/CON SỐ (KHÔNG DÙNG "Sản phẩm A, B", KHÔNG TỰ Ý ĐOÁN MÒ SỐ TIỀN).
4. Trả lời bằng tiếng Việt chuyên nghiệp, văn phong mạch lạc, tường minh. Dùng đơn vị tiền là BRL (ví dụ: 1.08 triệu BRL hoặc 8.45 triệu BRL).
5. Nếu kết quả ít dữ liệu (<3 dòng), vẫn phân tích đầy đủ.
6. Dùng emoji để trực quan hóa báo cáo.
"""
