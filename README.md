# Enterprise Data Lakehouse & MLOps Platform (with Agentic AI NL2SQL)

Dự án Data Lakehouse đạt chuẩn doanh nghiệp (Enterprise-grade) áp dụng kiến trúc **Medallion Architecture** (Bronze, Silver, Gold) kết hợp với hệ sinh thái **MLOps** và một hệ thống **LangGraph Multi-Agent AI Chatbot** thông minh có khả năng tự sửa lỗi (Self-Healing) để trực tiếp hỏi đáp về dữ liệu lớn.

---

## 🏛️ Kiến trúc Tổng thể & Công nghệ (Tech Stack)

### 1. Trụ cột Data Engineering (Lakehouse Infrastructure)
- **Object Storage:** [MinIO](https://min.io/) (Tương thích S3, lưu trữ dữ liệu dưới dạng Parquet/Columnar).
- **Table Format:** [Apache Iceberg](https://iceberg.apache.org/) mang lại khả năng xử lý giao dịch ACID, Time Travel và Schema Evolution trên Data Lake.
- **Metadata Catalog:** [Hive Metastore](https://hive.apache.org/) (sử dụng PostgreSQL làm backend).
- **Data Processing (ETL):** [Apache Spark](https://spark.apache.org/) (PySpark) đóng vai trò thực thi ETL qua 3 tầng:
  - **Bronze Layer:** Lưu trữ dữ liệu thô (Raw data từ Olist Batch & Kafka Streaming).
  - **Silver Layer:** Làm sạch, chuẩn hóa, khử trùng lặp và lưu trữ theo Rule-based ETL (`silver_rules.yaml`).
  - **Gold Layer:** Tổng hợp, tính toán các metrics phục vụ Business Intelligence và AI Engine (`fact_order`, `dim_customer`, `agg_daily_funnel`...).
- **Query Engine:** [Trino](https://trino.io/) - Truy vấn phân tán tốc độ cao trực tiếp trên bảng Iceberg.
- **Data Orchestration:** [Apache Airflow](https://airflow.apache.org/) điều phối toàn bộ các luồng ETL, luồng Streaming 15 phút/lần và tự động bảo trì bảng (Compaction & Vacuum).
- **BI & Dashboard:** [Apache Superset](https://superset.apache.org/).

### 2. Trụ cột AI & Agentic Workflow (NL2SQL Engine)
- **Multi-Agent Framework:** [LangGraph](https://python.langchain.com/docs/langgraph/) điều phối 4 Agents chuyên biệt: `Supervisor`, `Retrieval Agent`, `SQL Agent`, `Analyst Agent`.
- **Advanced RAG & Vector Storage:** [ChromaDB](https://docs.trychroma.com/) lưu trữ Schema và Few-shot SQL samples.
- **State-of-the-Art Optimization:**
  - **Cross-Encoder Reranker:** Sắp xếp và chọn lọc bảng chính xác 100%.
  - **Column Pruner:** Tỉa cột thông minh để tối ưu chi phí token LLM.
  - **Self-Healing (Tự sửa sai):** AI tự động bắt lỗi từ engine Trino và viết lại SQL tối đa 3 lần.
  - **AI Guardrails:** Node Supervisor tự động phát hiện, ngăn chặn SQL Injection (DROP/DELETE) và câu hỏi ngoài lề (Hallucination).

### 3. Trụ cột MLOps (Revenue Forecasting)
- **Model Tracking & Registry:** [MLflow](https://mlflow.org/) quản lý toàn bộ vòng đời huấn luyện mô hình.
- **Champion Models:** So sánh toàn diện 3 mô hình hàng đầu: **LightGBM** (Tree-based), **Prophet** (Time Series), **LSTM** (Deep Learning).
- **Chuẩn Doanh nghiệp MLOps:**
  - **No Data Leakage:** Cấu trúc **Time-based Split** đảm bảo mô hình không bị rò rỉ dữ liệu tương lai.
  - **Data Drift Monitoring:** Script `drift_monitor.py` đo lường chỉ số **PSI** và **Z-Score** hàng tháng trên Airflow để kích hoạt Retrain tự động khi phân phối doanh thu thay đổi.

---

## 🚀 Hướng dẫn cài đặt và chạy dự án

### Yêu cầu hệ thống
- Máy tính đã cài đặt sẵn **Docker** và **Docker Compose**.
- Cài đặt **Python 3.10+**.

### Bước 1: Khởi động nền tảng hạ tầng (Docker Compose)
Mở terminal, di chuyển vào thư mục `infrastructure` và chạy các container:

```bash
cd infrastructure
docker-compose up -d
```
*Các dịch vụ chạy ngầm bao gồm: MinIO, Trino, Airflow, MLflow, Superset, Hive Metastore và PostgreSQL.*

### Bước 2: Cấu hình môi trường
Tạo file `.env` ở thư mục gốc của dự án và điền các cấu hình cần thiết (Google API Key cho AI Bot, TRINO_PORT=8085...).

```bash
conda activate dlh
pip install -r requirements.txt
```

### Bước 3: Chạy quy trình xử lý dữ liệu (ETL Batch Pipeline)
Thực thi tuần tự các script Spark để build dữ liệu lên các layer (Bronze -> Silver -> Gold):

```bash
python -m src.etl.bronze.bronze_assets
python -m src.etl.silver.silver_assets
python -m src.etl.gold.gold_assets
```

### Bước 4: Chạy ứng dụng AI Chatbot
Mở 2 terminal để chạy song song Backend và Frontend:

**1. Chạy Chatbot Backend (FastAPI - Cổng 8000):**
```bash
uvicorn src.chatbot.backend.app:app --host 0.0.0.0 --port 8000
```

**2. Chạy Chatbot Frontend (Streamlit - Cổng 8501):**
```bash
streamlit run src/chatbot/frontend/ui.py
```

---

## 📊 Kịch bản Thực nghiệm & Đánh giá (Thesis Benchmarks)

Hệ thống tích hợp trọn bộ công cụ tự động đo lường và chứng minh kiến trúc tại thư mục `experiments/`:

**1. Chạy toàn bộ 4 cụm Benchmark (ETL, NL2SQL, Ablation, ML Models):**
```bash
python -m experiments.run_all
```
*(Kết quả tự động lưu tại `experiments/results/*.csv`)*

**2. Trích xuất bằng chứng năng lực Iceberg & Trino (EXPLAIN ANALYZE, Bảng $files):**
```bash
python -m experiments.iceberg_proof
```

**3. Kiểm chứng Chuẩn Doanh nghiệp (AI Security Guardrails, MLOps No Leakage):**
```bash
python -m experiments.enterprise_defense_proof
```

---

## 🌐 Các dịch vụ & Cổng truy cập (Services & Ports)

Sau khi khởi động thành công, bạn có thể truy cập các dịch vụ thông qua UI web tương ứng:

| Dịch vụ | Địa chỉ truy cập | Ghi chú |
| :--- | :--- | :--- |
| **Chatbot UI** | `http://localhost:8501` | Giao diện hỏi đáp dữ liệu (Streamlit) |
| **Chatbot API** | `http://localhost:8000/docs` | Swagger UI của FastAPI backend |
| **Airflow UI** | `http://localhost:8082` | Quản lý DAGs (ETL, Streaming, Maintenance, Retraining) |
| **MLflow UI** | `http://localhost:5000` | UI quản lý & so sánh mô hình MLOps |
| **Trino UI** | `http://localhost:8085` | Engine truy vấn SQL phân tán |
| **MinIO Console** | `http://localhost:9001` | Quản lý file/object storage |
| **Superset** | `http://localhost:8088` | BI Dashboard trực quan hóa dữ liệu |
