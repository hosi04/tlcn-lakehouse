INTENT_PROMPT = """\
Bạn là bộ phân loại ý định cho chatbot phân tích dữ liệu Lakehouse.

Phân loại câu hỏi sau vào MỘT trong 3 nhóm:
- "data_query": Câu hỏi yêu cầu truy vấn, phân tích, thống kê dữ liệu (doanh thu, đơn hàng, sản phẩm, khách hàng, seller, thời gian, ...)
- "greeting": Chào hỏi, hỏi thăm, giới thiệu bản thân
- "out_of_scope": Câu hỏi không liên quan đến dữ liệu (thời tiết, code, văn học, ...)

Câu hỏi: {question}

Trả lời ĐÚNG MỘT từ: data_query | greeting | out_of_scope
Nếu là greeting hoặc out_of_scope, thêm dòng thứ 2 là câu trả lời ngắn gọn bằng tiếng Việt.

Ví dụ output:
data_query
---
greeting
Xin chào! Tôi là trợ lý phân tích dữ liệu Lakehouse. Hãy hỏi tôi về doanh thu, đơn hàng, sản phẩm nhé!
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
2. Luôn dùng tên bảng đầy đủ: iceberg.gold.<table_name>
3. Luôn dùng alias khi JOIN nhiều bảng (ví dụ: fo, dc, d, dp, ds, foi)
4. Không dùng SELECT * — chỉ select cột cần thiết
5. Đặt alias rõ ràng cho aggregation (SUM(...) AS total_revenue)
6. Join fact → dimension bằng surrogate keys (_key), KHÔNG dùng business keys (_id)
7. Khi filter/group theo thời gian, JOIN với dim_date bằng date_key tương ứng
8. Trả về SQL THUẦN — không có giải thích, không có markdown code block

QUY TẮC ĐẶC THÙ SCHEMA NÀY (BẮT BUỘC):
- fact_order_item KHÔNG có cột 'quantity', 'qty', hay 'amount'.
  → Dùng COUNT(foi.order_item_key) hoặc COUNT(*) để đếm số lượng item bán.
- fact_order_item có các cột đo lường: item_price, item_freight_value, item_total_value.
- fact_order có: total_payment_value, total_product_value, total_freight_value, number_of_items.
- Để lọc đơn hàng thành công: WHERE order_status = 'delivered'

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

Trả về SQL ĐÃ SỬA, THUẦN (không giải thích, không markdown):
SQL:
"""


VIZ_SELECT_PROMPT = """\
Bạn là chuyên gia trực quan hóa dữ liệu. Dựa vào câu hỏi và cấu trúc dữ liệu, chọn loại biểu đồ phù hợp nhất.

CÂU HỎI: {question}

CỘT DỮ LIỆU: {columns}

SỐ DÒNG: {row_count}

MẪU DỮ LIỆU (5 dòng đầu):
{data_sample}

LOẠI BIỂU ĐỒ CÓ THỂ CHỌN:
- "line": Xu hướng theo thời gian (year, month, quarter, week)
- "bar": So sánh các nhóm, danh mục (category, product, seller, state)  
- "bar_horizontal": So sánh nhiều nhóm với nhãn dài
- "pie": Phân phối tỷ lệ (payment type, order status, category %)
- "scatter": Tương quan giữa 2 biến số liên tục
- "kpi": Chỉ có 1 giá trị số duy nhất (COUNT, SUM đơn lẻ)
- "table": Dữ liệu nhiều cột phức tạp, không phù hợp chart

OUTPUT FORMAT (JSON thuần, không giải thích):
{{
  "chart_type": "<type>",
  "x": "<tên cột trục x hoặc null>",
  "y": "<tên cột trục y hoặc list tên cột hoặc null>",
  "color": "<tên cột phân nhóm màu hoặc null>",
  "title": "<tiêu đề biểu đồ tiếng Việt>",
  "x_label": "<nhãn trục x tiếng Việt hoặc null>",
  "y_label": "<nhãn trục y tiếng Việt hoặc null>"
}}
"""


REPORT_PROMPT = """\
Bạn là chuyên gia phân tích dữ liệu. Dựa trên kết quả truy vấn SQL, hãy viết một báo cáo phân tích ngắn gọn bằng tiếng Việt.

CÂU HỎI CỦA NGƯỜI DÙNG: {question}

SQL ĐÃ CHẠY:
```sql
{sql}
```

KẾT QUẢ ({row_count} dòng):
{data_text}

YÊU CẦU BÁO CÁO:
1. Bắt đầu bằng **kết quả chính** (số liệu nổi bật nhất)
2. Nêu **xu hướng hoặc pattern** nếu có (tăng/giảm, cao nhất/thấp nhất)
3. **Nhận xét ngắn** về insight quan trọng
4. Nếu dữ liệu nhiều dòng, chỉ đề cập Top 3–5 kết quả nổi bật
5. Viết súc tích, chuyên nghiệp, dùng số liệu cụ thể, format Markdown
6. KHÔNG đề cập đến SQL hay kỹ thuật — chỉ nói về kết quả kinh doanh

Báo cáo:
"""
