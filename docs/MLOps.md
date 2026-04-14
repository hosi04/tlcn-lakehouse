# Kế hoạch Triển khai MLOps cho Olist Lakehouse

Bản kế hoạch này định hình kiến trúc và các bước triển khai phần Machine Learning / MLOps cho 3 bài toán phân tích dựa vào phân tích mới nhất:
1. Dự báo doanh thu (Time Series Forecasting)
2. Phát hiện bất thường (Anomaly Detection)
3. Dự báo giao trễ (Sequence Classification)

> [!IMPORTANT]
> Toàn bộ cả 3 bài toán đều sử dụng dữ liệu từ tầng **Gold (Star Schema)** của Iceberg thay vì Silver. Việc này đảm bảo tính nhất quán (Single Source of Truth) và tận dụng các bảng Dimension (dim_date, dim_seller) cùng các metrics đã được pre-calculate trong Fact (fact_order).

## 1. Kiến trúc thư mục MLOps đề xuất

Tạo không gian làm việc mới tách biệt nhưng vẫn tận dụng được Data Pipeline hiện có:

```text
src/
└── mlops/
    ├── __init__.py
    ├── data_loader.py          # Kết nối PySpark -> Iceberg Gold -> Pandas DataFrame
    ├── utils/
    │   └── mlflow_setup.py     # Thiết lập kết nối Tracking Server
    ├── p1_revenue/             # Bài toán 1
    │   ├── train.py            # Train LSTM & LightGBM, so sánh trên MLflow
    │   └── model.py            # Định nghĩa kiến trúc model
    ├── p2_anomaly/             # Bài toán 2 
    │   ├── train.py            # Train Simple Autoencoder
    │   └── model.py            
    └── p3_late_delivery/       # Bài toán 3
        ├── train.py            # Train LSTM Classifier theo chuỗi thời gian của Seller
        └── model.py            
```

## 2. Chi tiết từng Bài toán cụ thể

### 2.1. Bài toán 1: Dự báo Doanh thu (Revenue Forecasting)
- **Nguồn dữ liệu**: `fact_order` JOIN `dim_date`
- **Features chính**: `total_payment_value` (Target), `week_of_year`, `month`, `is_weekend`
- **Chiến lược MLOps**: 
  - Train đồng thời 2 model: **LSTM** (Deep Learning) và **LightGBM** phối hợp với Lag Features.
  - Sử dụng **MLflow** để log lại các metrics (RMSE, MAE) của cả 2 model trong cùng một Experiment.
  - So sánh trực tiếp trên giao diện MLflow để quyết định model nào sẽ đưa lên Production (Log Model Registry). Đây là điểm "ăn tiền" khi bảo vệ đồ án.

### 2.2. Bài toán 2: Phát hiện Bất thường (Anomaly Detection)
- **Nguồn dữ liệu**: `fact_order`
- **Features chính**: `delivery_actual_days`, `delivery_estimate_days`, `total_freight_value`, `total_product_value`, `number_of_items`
- **Chiến lược MLOps**:
  - Xây dựng một **Autoencoder đơn giản** (Dense layer 5 -> 3 -> 5) bằng PyTorch hoặc Keras.
  - Huấn luyện trên toàn bộ tập dữ liệu (với giả định <5% là bất thường).
  - Lấy Reconstruction Error làm ngưỡng (Threshold) phân loại bất thường.
  - MLflow logging: log lại Distribution của điểm lỗi và lưu PyTorch/Tensorflow model.

### 2.3. Bài toán 3: Dự báo Giao trễ (Late Delivery Prediction)
- **Nguồn dữ liệu**: `fact_order` JOIN `dim_seller` JOIN `dim_date`
- **Xử lý chuỗi**: Nhóm dữ liệu theo `seller_key` và sort theo `date_key`. Tạo chuỗi các đơn hàng liên tiếp của điểm bán.
- **Chiến lược MLOps**:
  - Khai báo model **LSTM Classifier**. 
  - Do hạn chế về dữ liệu lịch sử trên từng Seller, sẽ tiền xử lý bỏ qua Seller có dưới N đơn vị lịch sử (ví dụ: N=10).
  - Label: `delivery_early_days` < 0 (Trễ).
  - MLflow: Log lại các Precision, Recall, F1-Score do dữ liệu Imbalanced (số đơn trễ ít hơn đơn đúng hạn).

## 3. Workflow Pipeline (PySpark → MLflow)

1. **Extract**: Các script train gọi `src.mlops.data_loader` để dùng Spark Session lấy dữ liệu từ `iceberg.gold.*`.
2. **Transform**: Chuyển đổi Spark DataFrame sang Pandas (`df.toPandas()`) (Vì tập data Olist hoàn toàn fit trong RAM máy tính khi tính năng ở Gold layer) để tiến hành StandardScaling, Windowing.
3. **Train & Log**: Script gọi thư viện ML tương ứng và bọc bằng `mlflow.start_run()`. 
4. **Register**: Tự động register model tốt nhất dựa trên validation metric.

## Open Questions
- Thư viện Deep Learning bạn mong muốn sử dụng cho LSTM và Autoencoder là gì? (PyTorch hay TensorFlow/Keras)? **Khuyến nghị**: PyTorch phù hợp trong môi trường học thuật, Keras dễ code nhanh.
- Quá trình chạy MLflow server: Bạn muốn Start cục bộ (sqlite backend / local file) song song với MinIO cho Artifacts giống như lúc thiết lập pipeline trước đó không?

## Verification Plan
1. Viết `data_loader.py` và Test Data Fetching: Phải JOIN và lấy thành công Pandas DataFrame mà không làm sập RAM từ Iceberg.
2. Thiết lập MLflow server (nếu chưa chạy) và verify việc ghi log.
3. Triển khai script train Bài toán 1 trước để xác nhận việc chạy song song 2 Model và so sánh trên UI của MLflow.
