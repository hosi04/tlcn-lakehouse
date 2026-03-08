# tlcn-lakehouse

Dự án Data Lakehouse áp dụng kiến trúc **Medallion Architecture** (Bronze, Silver, Gold) kết hợp với một hệ thống **AI Chatbot** thông minh để trực tiếp hỏi đáp về dữ liệu. Dự án được xây dựng dựa trên các công nghệ Big Data hiện đại nhất.

## Kiến trúc & Công nghệ (Tech Stack)

### 1. Data Lakehouse Infrastructure
- **Object Storage:** [MinIO](https://min.io/) (Tương thích S3).
- **Table Format:** [Apache Iceberg](https://iceberg.apache.org/) mang lại khả năng xử lý ACID trên Data Lake.
- **Metadata Catalog:** [Hive Metastore](https://hive.apache.org/) (sử dụng PostgreSQL làm backend).
- **Data Processing:** [Apache Spark](https://spark.apache.org/) (PySpark) đóng vai trò thực thi ETL qua 3 tầng:
  - **Bronze Layer:** Lưu trữ dữ liệu thô (Raw data).
  - **Silver Layer:** Làm sạch, chuẩn hóa và tối ưu dữ liệu.
  - **Gold Layer:** Tổng hợp, tính toán các metrics phục vụ Business Intelligence.
- **Query Engine:** [Trino](https://trino.io/) - Truy vấn phân tán tốc độ cao trực tiếp trên Iceberg.
- **Data Visualization / BI:** [Apache Superset](https://superset.apache.org/).

### 2. AI Chatbot
- **Backend:** [FastAPI](https://fastapi.tiangolo.com/), [LangChain](https://www.langchain.com/), `langchain-google-genai` kết nối tới Trino để chuyển đổi Text-to-SQL và lấy kết quả.
- **Frontend:** [Streamlit](https://streamlit.io/) tạo giao diện UI chat trực quan.

---

## Hướng dẫn cài đặt và chạy dự án

### Yêu cầu hệ thống
- Máy tính đã cài đặt sẵn **Docker** và **Docker Compose**.
- Cài đặt **Python 3.x**.

### Bước 1: Khởi động nền tảng hạ tầng (Infrastructure)
Mở terminal, di chuyển vào thư mục `infrastructure` và chạy các container:

```bash
cd infrastructure
docker-compose up -d
```
*Các dịch vụ sẽ được chạy dưới nền, bao gồm: MinIO, Trino, Superset, Hive Metastore và PostgreSQL.*

### Bước 2: Khởi tạo biến môi trường
Tạo file `.env` ở thư mục gốc của dự án dựa trên template `.env` (hoặc `.env.example`) và điền các API Key cần thiết (ví dụ: Google API Key cho LangChain).

### Bước 3: Cài đặt thư viện Python
Mở một terminal mới ở thư mục gốc dự án và chạy:

```bash
pip install -r requirements.txt
```

### Bước 4: Chạy quy trình xử lý dữ liệu (ETL Pipeline)
Thực thi tuần tự các script Spark để build dữ liệu lên các layer (Bronze -> Silver -> Gold):

```bash
python -m src.spark.bronze.bronze_assets
python -m src.spark.silver.silver_assets
python -m src.spark.gold.gold_assets
```

### Bước 5: Chạy ứng dụng AI Chatbot
Bạn cần mở 2 terminal để chạy song song Backend và Frontend:

**1. Chạy Chatbot Backend (FastAPI):**
```bash
uvicorn src.chatbot.backend.app:app --reload --port 5000
```

**2. Chạy Chatbot Frontend (Streamlit):**
```bash
streamlit run src/chatbot/frontend/ui.py
```

---

## Các dịch vụ (Services & Ports)

Sau khi khởi động thành công, bạn có thể truy cập các dịch vụ thông qua UI web tương ứng:

| Dịch vụ | Địa chỉ truy cập | Ghi chú |
| :--- | :--- | :--- |
| **Chatbot UI** | `http://localhost:8501` | Giao diện hỏi đáp dữ liệu (Streamlit) |
| **Chatbot API** | `http://localhost:5000/docs` | Swagger UI của FastAPI backend |
| **MinIO Console** | `http://localhost:9001` | Quản lý file/object storage |
| **Trino** | `http://localhost:8080` | UI quản lý câu query Trino |
| **Superset** | `http://localhost:8088` | BI Dashboard trực quan hóa dữ liệu |

*Trino Connection URI tích hợp trong code:* `trino://trino@trino:8080/iceberg`
