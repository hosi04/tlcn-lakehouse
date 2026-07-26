# 🚀 Enterprise Data Lakehouse & MLOps Platform with Self-Healing Agentic AI (NL2SQL)

[![Data Engineering](https://img.shields.io/badge/Data_Engineering-Medallion_Architecture-blue.svg)](#-1-trụ-cột-data-engineering--lakehouse-infrastructure)
[![AI Agent](https://img.shields.io/badge/AI_Agent-LangGraph_Multi--Agent-orange.svg)](#-2-trụ-cột-agentic-ai--advanced-nl2sql-engine)
[![MLOps](https://img.shields.io/badge/MLOps-MLflow_%26_Drift_Monitoring-green.svg)](#-3-trụ-cột-mlops--revenue-forecasting-platform)
[![NL2SQL Accuracy](https://img.shields.io/badge/NL2SQL_Pass_Rate-93%25-brightgreen.svg)](#-kiểm-chứng-thực-nghiệm--thesis-benchmarks)

Dự án Nền tảng **Enterprise Data Lakehouse & MLOps Platform** kết hợp hệ thống **LangGraph Multi-Agent Chatbot AI** có khả năng tự sửa lỗi (Self-Healing NL2SQL), phục vụ phân tích dữ liệu lớn và dự báo doanh thu thời gian thực. Hệ thống áp dụng kiến trúc **Medallion Architecture (Bronze - Silver - Gold)** chuẩn doanh nghiệp, tối ưu hoá với **Apache Iceberg**, **Trino**, **Apache Airflow**, **MLflow** và **Kafka Streaming**.

---

## 📌 Mục lục
- [🌟 Điểm nổi bật của Dự án (Executive Highlights)](#-điểm-nổi-bật-của-dự-án-executive-highlights)
- [🏛️ Kiến trúc Tổng thể Hệ thống (System Architecture)](#️-kiến-trúc-tổng-thể-hệ-thống-system-architecture)
- [💻 Công nghệ Sử dụng (Tech Stack)](#-công-nghệ-sử-dụng-tech-stack)
- [🗄️ 1. Trụ cột Data Engineering & Lakehouse Infrastructure](#️-1-trụ-cột-data-engineering--lakehouse-infrastructure)
- [🤖 2. Trụ cột Agentic AI & Advanced NL2SQL Engine](#-2-trụ-cột-agentic-ai--advanced-nl2sql-engine)
- [📈 3. Trụ cột MLOps & Revenue Forecasting Platform](#-3-trụ-cột-mlops--revenue-forecasting-platform)
- [📊 Kiểm chứng Thực nghiệm & Thesis Benchmarks](#-kiểm-chứng-thực-nghiệm--thesis-benchmarks)
- [🌐 Các Dịch vụ & Cổng Truy cập (Services & Ports)](#-các-dịch-vụ--cổng-truy-cập-services--ports)
- [🚀 Hướng dẫn Cài đặt và Vận hành (Deployment Guide)](#-hướng-dẫn-cài-đặt-và-vận-hành-deployment-guide)
- [📂 Cấu trúc Thư mục Dự án (Project Structure)](#-cấu-trúc-thư-mục-dự-án-project-structure)

---

## 🌟 Điểm nổi bật của Dự án (Executive Highlights)

* **Enterprise Medallion Lakehouse**: Xử lý hoàn chỉnh luồng Batch (Olist E-Commerce Dataset) và Streaming (Kafka Real-time User Events) trên 3 tầng Bronze (Raw Parquet), Silver (Rule-based Cleansing via PySpark & MERGE), Gold (Star-Schema Analytics Tables).
* **High-Performance Querying & ACID Transactions**: Sử dụng **Apache Iceberg** trên **MinIO Object Storage** kết hợp với **Trino Engine** cho khả năng truy vấn SQL phân tán cực nhanh, hỗ trợ ACID Transactions, Time Travel và Schema Evolution.
* **Tự động Bảo trì Data Lake**: Tích hợp các Airflow DAGs tự động chạy **Compaction** (gộp file nhỏ) và **Vacuum** (xóa snapshot cũ), giảm thời gian truy vấn planning từ 42ms xuống 13ms (tối ưu gấp 3 lần).
* **Self-Healing Multi-Agent AI (NL2SQL)**: Xây dựng bằng **LangGraph** gồm 5 Agent chuyên biệt (`Supervisor`, `Retrieval`, `SQL Engine`, `Analyst`, `What-If`). Hệ thống tự động phát hiện lỗi cú pháp SQL từ Trino và thực hiện **Self-Healing loop tối đa 3 lần** trước khi trả kết quả.
* **Advanced RAG Optimization**: Sử dụng **ChromaDB Vector Indexing**, kết hợp **Cross-Encoder Reranker** (chọn bảng chuẩn xác 100%) và **Column Pruner** (tỉa bớt cột thừa để tiết kiệm token LLM).
* **AI Security Guardrails**: Ngăn chặn tuyệt đối SQL Injection (DROP/DELETE/UPDATE), Prompt Injection và từ chối các câu hỏi out-of-scope (Hallucination Defense).
* **End-to-End MLOps Pipeline**: Quản lý vòng đời mô hình bằng **MLflow**, so sánh 3 kiến trúc mô hình (**LightGBM**, **Prophet**, **LSTM**). Tự động giám sát **Data Drift (PSI & Z-Score)** trên Airflow để trigger Re-training tự động khi phân phối dữ liệu thay đổi.
* **Kiểm chứng Thực nghiệm Toàn diện**: Bộ test suite `experiments/run_all.py` đánh giá tự động trên 100 bộ câu hỏi NL2SQL benchmark đạt tỷ lệ thành công **93.0% Pass Rate**.

---

## 🏛️ Kiến trúc Tổng thể Hệ thống (System Architecture)

<img width="5195" height="2426" alt="Kien truc (2)" src="https://github.com/user-attachments/assets/766f8aff-1ad6-42bd-97d9-c5147645a594" />



---

## 💻 Công nghệ Sử dụng (Tech Stack)

| Phân khu | Công nghệ / Thư viện | Vai trò trong hệ thống |
| :--- | :--- | :--- |
| **Storage & Catalog** | **MinIO (S3)**, **Apache Iceberg**, **Hive Metastore**, **PostgreSQL** | Lưu trữ Parquet, quản lý Table Format chuẩn ACID, Time Travel & Schema Evolution |
| **Data Processing** | **PySpark 3.5**, **Trino Engine**, **Apache Kafka** | Thực thi ETL Batch & Real-time Streaming, truy vấn phân tán tốc độ cao |
| **Orchestration & MLOps** | **Apache Airflow**, **MLflow** | Điều phối Pipeline, theo dõi thí nghiệm, lưu trữ Model Artifacts & Drift Monitoring |
| **AI Framework & RAG** | **LangGraph**, **LangChain**, **ChromaDB**, **Sentence-Transformers** | Điều phối Multi-Agent, Vector Search, Cross-Encoder Reranker, Context Pruning |
| **LLM & Inference** | **Google Gemini API / Ollama** | LLM Engine sinh câu truy vấn SQL và phân tích báo cáo doanh nghiệp |
| **ML Models** | **Prophet**, **LightGBM**, **LSTM (PyTorch/TensorFlow)** | Dự báo doanh thu chuỗi thời gian (Time-Series Revenue Forecasting) |
| **Backend & Frontend** | **FastAPI**, **Streamlit**, **Apache Superset**, **Plotly** | REST API Service, UI hỏi đáp dữ liệu, Dashboard BI trực quan hóa |

---

## 🗄️ 1. Trụ cột Data Engineering & Lakehouse Infrastructure

<img width="1507" height="326" alt="image" src="https://github.com/user-attachments/assets/fe848025-d327-4877-837e-49942caf060f" />


### 1.1 Medallion Architecture (Bronze -> Silver -> Gold)
1. **Bronze Layer (Raw Storage)**:
   - Lưu trữ dữ liệu nguyên bản từ tập dữ liệu Olist E-Commerce (99,441 đơn hàng, 112,650 items, khách hàng, người bán, sản phẩm) và Kafka Stream under Parquet format.
2. **Silver Layer (Cleaned & Standardized)**:
   - Lọc bỏ trùng lặp, xử lý giá trị khuyết thiếu (Null Handling), chuẩn hóa kiểu dữ liệu Date/Timestamp và áp dụng bộ quy tắc làm sạch theo định nghĩa `silver_rules.yaml`.
   - Đối với dữ liệu sự kiện streaming (`events`), áp dụng phương thức **Incremental MERGE** theo khoảng thời gian 5 phút/lần.
3. **Gold Layer (Data Marts & Star Schema)**:
   - Xây dựng mô hình hình sao (Star-Schema) bao gồm các bảng Fact và Dimension: `fact_order`, `fact_order_item`, `dim_customer`, `dim_seller`, `dim_product`, `dim_date`, `agg_daily_funnel`.
   - Phục vụ trực tiếp cho Trino Query Engine, Superset BI và MLOps Data Loader.

### 1.2 Tối ưu hóa Data Lake (Iceberg Compaction & Vacuum)
* **Vấn đề Small-File Fragmentation**: Việc nạp dữ liệu streaming tạo ra nhiều file Parquet nhỏ (dung lượng vài KB - vài MB), làm tăng chi phí Metadata Scan của Trino.
* **Giải pháp**: Tích hợp Airflow Maintenance DAG thực thi lệnh compaction:
  ```sql
  ALTER TABLE iceberg.silver.orders EXECUTE optimize (file_size_threshold => '10MB');
  ALTER TABLE iceberg.silver.orders EXECUTE expire_snapshots(retention_threshold => '7d');
  ```
* **Kết quả Benchmark**: Gộp **32 file nhỏ thành 1 file Parquet tối ưu (~4.8MB)**, giảm thời gian query planning xuống 3 lần.

---

## 🤖 2. Trụ cột Agentic AI & Advanced NL2SQL Engine

<img width="4189" height="2971" alt="Kien truc Chatbot" src="https://github.com/user-attachments/assets/f3910725-e6db-4258-ad7b-e58afb066226" />

1. **Supervisor Agent (Security & Routing)**: Phân loại ý định (`data_query`, `what_if_simulation`, `off_topic`) và kiểm tra AI Guardrails (chống SQL Injection `DROP`, `DELETE`, `TRUNCATE`, ngăn chặn Prompt Injection).
2. **Retrieval Agent (Advanced RAG)**:
   - **Multi-Query Generator**: Tự động mở rộng câu hỏi của người dùng thành các góc nhìn khác nhau.
   - **Vector Schema Search**: Tìm kiếm Schema bảng phù hợp từ ChromaDB.
   - **Cross-Encoder Reranker**: Đánh giá lại độ liên quan của Bảng/Cột, nâng tỷ lệ chọn đúng bảng lên 100%.
   - **Column Pruner**: Tỉa bỏ các cột không liên quan, giảm hơn 40% chi phí Token LLM.
3. **SQL Agent (Self-Healing Execution)**:
   - Tự động sinh SQL tương thích dialect của **Trino**.
   - Thực thi truy vấn lên Trino. Nếu Trino trả về lỗi Syntax/Schema, nút **Self-Healing** sẽ bắt traceback lỗi, cung cấp cho LLM sửa lại câu SQL (Tối đa 3 vòng lặp).
4. **Analyst Agent**: Tổng hợp kết quả bảng dữ liệu, đưa ra nhận xét kinh doanh (Business Insights) và tự động tạo đồ thị trực quan hóa bằng **Plotly**.
5. **What-If Agent**: Tiếp nhận câu hỏi giả định kinh doanh (ví dụ: *"Nếu tuần tới tăng 10% lượng đơn hàng thì doanh thu là bao nhiêu?"*) và kết nối trực tiếp với API MLOps để trả kết quả.

---

## 📈 3. Trụ cột MLOps & Revenue Forecasting Platform

<img width="1482" height="671" alt="Screenshot 2026-06-27 182128" src="https://github.com/user-attachments/assets/e5370a4f-4982-4753-aae2-c1a68c11a164" />


### 3.1 Vòng đời Huấn luyện & So sánh Mô hình (MLflow Integration)
Hệ thống thử nghiệm và so sánh đồng thời 3 kiến trúc mô hình dự báo doanh thu chuỗi thời gian (Daily/Weekly Revenue):

| Model Architecture | Thuật toán / Nguyên lý | Ưu điểm | MAE (BRL) | RMSE (BRL) | Trạng thái |
| :--- | :--- | :--- | :---: | :---: | :---: |
| **Prophet** | Additive Time-Series (Trend, Seasonality, Holidays) | Bắt nhịp chu kỳ theo tuần/năm cực tốt, chịu được dữ liệu khuyết | **12,459** | **14,860** | 🏆 **Champion Model** |
| **LightGBM** | Gradient Boosted Decision Trees (Feature Lag/Rolling) | Tốc độ huấn luyện nhanh, bắt tương quan phi tuyến | 19,983 | 23,296 | Contender |
| **LSTM** | Deep Learning Recurrent Neural Network (PyTorch) | Học chuỗi thời gian phụ thuộc dài hạn | 56,844 | 67,163 | Baseline |

*Tất cả thông số Hyperparameters, Loss metrics và Model Binary được tự động lưu trữ lên MLflow Server (`http://localhost:5000`) và MinIO S3 Bucket `s3://mlflow/`.*

### 3.2 Automated Retraining & Data Drift Monitoring
* **Đảm bảo Không Rò rỉ Dữ liệu (No Data Leakage)**: Áp dụng **Time-based Train/Test Split** nghiêm ngặt cho dữ liệu chuỗi thời gian.
* **Giám sát Phân phối Dữ liệu (Drift Detection)**: File `drift_monitor.py` đo lường chỉ số **PSI (Population Stability Index)** và **Z-Score** giữa phân phối doanh thu tháng trước và tháng hiện tại.
* **Tự động Retrain qua Airflow**: Khi `PSI > 0.2` (Phân phối dữ liệu thay đổi đáng kể), Airflow DAG `model_retraining_dag.py` tự động kích hoạt Pipeline huấn luyện lại mô hình và cập nhật Model Register trên MLflow.

---

## 📊 Kiểm chứng Thực nghiệm & Thesis Benchmarks

Dự án đi kèm bộ thực nghiệm tự động hóa (`experiments/run_all.py`) để kiểm chứng năng lực của hệ thống theo tiêu chuẩn nghiên cứu khoa học và sản phẩm doanh nghiệp:

<img width="1106" height="700" alt="Screenshot 2026-07-09 163019" src="https://github.com/user-attachments/assets/0194ff47-60a9-47da-90f8-19ffac6fd576" />


### 1. Đánh giá Khả năng sinh SQL (NL2SQL Benchmark - 100 Test Cases)

| Mức độ khó (Difficulty) | Số lượng câu hỏi | Đạt (Passed) | Thất bại (Failed) | Tỷ lệ thành công (Pass Rate) | Ngưỡng yêu cầu | Trạng thái | Thời gian phản hồi TB |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Easy** (1 bảng, aggregate đơn giản) | 30 | 30 | 0 | **100.0%** | 90.0% | **PASS** | 13.88s |
| **Medium** (JOIN 2-3 bảng, GROUP BY) | 40 | 40 | 0 | **100.0%** | 75.0% | **PASS** | 17.29s |
| **Hard** (Subquery, Window function, Complex Join) | 30 | 23 | 7 | **76.7%** | 50.0% | **PASS** | 23.22s |
| **TỔNG CỘNG** | **100** | **93** | **7** | **93.0%** | **70.0%** | **PASS** | **18.05s** |

### 2. Đánh giá Hiệu năng Data Lakehouse & Truy vấn Trino

| Bảng dữ liệu | Số lượng bản ghi (Rows) | Số cột (Cols) | Thời gian truy vấn Trino (s) | Trạng thái |
| :--- | :---: | :---: | :---: | :---: |
| `gold.fact_order` | 98,666 | 17 | **0.122s** | OK |
| `gold.fact_order_item` | 112,650 | 14 | **0.097s** | OK |
| `gold.dim_customer` | 99,441 | 7 | **0.121s** | OK |
| `silver.order_reviews` | 99,227 | 6 | **0.138s** | OK |

---

## 🌐 Các Dịch vụ & Cổng Truy cập (Services & Ports)

Sau khi khởi động toàn bộ hạ tầng bằng Docker, bạn có thể truy cập các bảng điều khiển quản trị qua Web Browser:

| Dịch vụ (Service) | URL Truy cập | Tài khoản mặc định | Mô tả chức năng |
| :--- | :--- | :--- | :--- |
| **Chatbot Frontend** | `http://localhost:8501` | *-* | Giao diện hỏi đáp dữ liệu & đồ thị trực quan (Streamlit) |
| **Chatbot API Docs** | `http://localhost:8000/docs` | *-* | OpenAPI / Swagger UI của FastAPI Chatbot Backend |
| **MLOps Serving API** | `http://localhost:8001/docs` | *-* | Swagger UI dịch vụ dự báo doanh thu & What-If Simulation |
| **Airflow Webserver** | `http://localhost:8082` | `airflow` / `airflow` | Bảng điều khiển quản lý DAGs (ETL, Streaming, Retraining) |
| **MLflow UI** | `http://localhost:5000` | *-* | Giao diện quản lý thử nghiệm & lưu trữ Model Artifacts |
| **Trino Query Engine** | `http://localhost:8085` | `trino` | UI theo dõi các câu truy vấn SQL phân tán thời gian thực |
| **MinIO Console** | `http://localhost:9001` | `minio` / `minio123` | Quản lý S3 Object Storage (Buckets: `iceberg`, `mlflow`) |
| **Kafka UI** | `http://localhost:8090` | *-* | Bảng giám sát Kafka Cluster, Topics & Event Streams |
| **Apache Superset** | `http://localhost:8088` | `superset` / `superset` | Nền tảng Business Intelligence (BI Dashboard) |

---

## 🚀 Hướng dẫn Cài đặt và Vận hành (Deployment Guide)

### Yêu cầu Tiền đề (Prerequisites)
* **Docker Engine** (v24.0+) & **Docker Compose** (v2.20+)
* **Python**: 3.10 hoặc 3.11 (Khuyến khích dùng Conda)
* RAM tối thiểu: **8GB - 16GB** (Khuyến nghị để chạy mượt toàn bộ hạ tầng Container Docker)

### Bước 1: Clone Repository & Khởi tạo Môi trường Python
```bash
# 1. Clone repository
git clone https://github.com/your-username/tlcn-lakehouse.git
cd tlcn-lakehouse

# 2. Tạo môi trường virtualenv/conda
conda create -n lakehouse python=3.10 -y
conda activate lakehouse

# 3. Cài đặt các thư viện phụ thuộc
pip install -r requirements.txt
```

### Bước 2: Khởi động Hạ tầng Docker Services
```bash
cd infrastructure
docker-compose up -d
```
> *Lưu ý: Chờ khoảng 1-2 phút để các service (Hive Metastore, Trino, Airflow, MinIO, MLflow) hoàn tất quá trình Healthcheck.*

### Bước 3: Cấu hình Biến môi trường (.env)
Tạo file `.env` tại thư mục gốc của dự án:
```env
# Google Gemini Key (Dành cho AI Chatbot Agent)
GEMINI_API_KEY=your_gemini_api_key_here

# Cấu hình Trino Engine
TRINO_HOST=localhost
TRINO_PORT=8085
TRINO_USER=trino
TRINO_CATALOG=iceberg

# Cấu hình MinIO / S3 Storage
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minio
MINIO_SECRET_KEY=minio123
```

### Bước 4: Thực thi Pipeline ETL (Nạp dữ liệu lên Lakehouse)
Thực thi các script PySpark để build dữ liệu qua các tầng Medallion Architecture:

```bash
# Nạp dữ liệu thô vào tầng Bronze
python -m src.etl.bronze.bronze_assets

# Làm sạch & chuẩn hóa lên tầng Silver (Iceberg format)
python -m src.etl.silver.silver_assets

# Aggregation & tạo Star-Schema lên tầng Gold
python -m src.etl.gold.gold_assets
```

### Bước 5: Khởi động Hệ thống Chatbot AI (Backend & Frontend)

**Mở Terminal 1 (Chạy FastAPI Chatbot Backend):**
```bash
uvicorn src.chatbot.backend.app:app --host 0.0.0.0 --port 8000
```

**Mở Terminal 2 (Chạy Streamlit Frontend UI):**
```bash
streamlit run src/chatbot/frontend/ui.py
```

### Bước 6: Khởi động MLOps Model Training & Serving (Tùy chọn)
```bash
# 1. Huấn luyện các mô hình dự báo doanh thu & log kết quả lên MLflow
python -m src.mlops.revenue.trainer_prophet
python -m src.mlops.revenue.trainer_lightgbm

# 2. Khởi động API MLOps Model Serving (Cổng 8001)
uvicorn src.mlops.serving.app:app --host 0.0.0.0 --port 8001
```

### Bước 7: Chạy Bộ Thử nghiệm Benchmarks
```bash
python -m experiments.run_all
```

---

## 📂 Cấu trúc Thư mục Dự án (Project Structure)

```text
tlcn-lakehouse/
├── airflow/                        # Apache Airflow DAGs
│   └── dags/                       # ETL, Maintenance & Retraining DAGs
├── config/                         # File cấu hình YAML (Silver Rules, Schema)
├── data/                           # Dữ liệu nguồn Olist CSV & Sample Events
├── experiments/                    # Bộ Benchmark & Đánh giá Thực nghiệm
│   ├── enterprise_defense_proof.py # Thử nghiệm Security Guardrails & Leakage
│   ├── etl_benchmark.py           # Benchmark thời gian xử lý ETL & Trino
│   ├── iceberg_proof.py            # Kiểm chứng Compaction & Small Files
│   ├── nl2sql_benchmark.py         # Benchmark 100 câu hỏi NL2SQL
│   └── results/                    # Kết quả xuất ra file CSV / Markdown
├── infrastructure/                 # Docker Compose & Cấu hình Docker
│   ├── docker-compose.yaml         # File định nghĩa 11 Services
│   ├── airflow/                    # Dockerfile & Conf Airflow
│   ├── hive-metastore/             # Hive Metastore Service
│   ├── minio/                      # S3 Storage MinIO Service
│   ├── mlflow/                     # MLflow Server Service
│   ├── superset/                   # Apache Superset Service
│   └── trino/                      # Trino Catalog & Node Config
├── src/                            # Mã nguồn chính của dự án
│   ├── chatbot/                    # Hệ thống LangGraph Multi-Agent AI Chatbot
│   │   ├── backend/                # FastAPI Backend & Agent Core
│   │   │   ├── agent/              # LangGraph Workflow, Prompts & Nodes
│   │   │   ├── retrieval/          # ChromaDB, Reranker & Column Pruner
│   │   │   └── business_rules.yaml # Quy tắc phân tích kinh doanh
│   │   └── frontend/               # Giao diện người dùng Streamlit
│   ├── etl/                        # Pipeline ETL & Data Lakehouse Logic
│   │   ├── bronze/                 # Ingestion dữ liệu thô (Batch & Streaming)
│   │   ├── silver/                 # Làm sạch dữ liệu (Rule Engine & MERGE)
│   │   ├── gold/                   # Tổng hợp dữ liệu (Star-Schema Data Marts)
│   │   ├── maintenance/            # Script Compaction & Vacuum Iceberg
│   │   └── streaming/              # Kafka Generator & Spark Streaming
│   └── mlops/                      # Hệ thống MLOps Dự báo Doanh thu
│       ├── revenue/                # Trainers (LightGBM, Prophet, LSTM) & Drift Monitor
│       └── serving/                # FastAPI Inference API cho What-If Agent
├── tests/                          # Automated Pytest Unit/Integration Tests
├── README.md                       # Tài liệu hướng dẫn dự án
└── requirements.txt                # Thư viện Python phụ thuộc
```

---

## 👨‍💻 Tác giả & Liên hệ (Author)

* **Họ và tên**: [Tên của bạn]
* **Vị trí Ứng tuyển**: Data Engineer / Data Architect / MLOps Engineer / AI Engineer
* **Email**: [Email của bạn]
* **LinkedIn**: [Link LinkedIn của bạn]
* **GitHub**: [Link GitHub Profile của bạn]

---
*⭐ Nếu bạn thấy dự án này hữu ích hoặc ấn tượng, hãy dành tặng dự án 1 Star trên GitHub nhé!*
