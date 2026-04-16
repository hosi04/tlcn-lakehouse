# Tích hợp Hệ thống Grid Search Hyperparameter Tuning cho Pipeline MLOps

Kế hoạch này sẽ tái cấu trúc các tệp huấn luyện (train.py) hiện tại của 3 bài toán MLOps. Thay vì cố định (hardcode) một bộ siêu tham số (Hyperparameters) duy nhất, hệ thống sẽ thực hiện Grid Search duyệt qua một tập hợp các kết hợp tham số, ghi nhận (log) tất cả các nhánh chạy này vào MLflow dưới dạng Nested Runs (Thử nghiệm lồng nhau), và cuối cùng chỉ đẩy (Register) phiên bản **Model tốt nhất** lên MLflow Model Registry.

## User Review Required

> [!IMPORTANT]
> Việc duyệt qua nhiều siêu tham số sẽ kéo dài thời gian huấn luyện tương ứng với số lượng cấu hình được tạo ra. Để đảm bảo đồ án chạy không quá lâu khi demo, các Grid mẫu dưới đây được giới hạn ở kích thước nhỏ (khoảng 4-6 cấu hình/mô hình). Xin vui lòng xác nhận xem bạn có muốn tăng/giảm kích thước Grid này hay thêm tham số cụ thể nào không.

## Proposed Changes

### 1. P1 - Revenue Forecasting
Triển khai Grid Search cho cả mô hình LightGBM và LSTM:
*   **Grid LightGBM:** `learning_rate` ∈ [0.01, 0.05], `num_leaves` ∈ [15, 31], `min_data_in_leaf` ∈ [5, 10]
*   **Grid LSTM:** `lr` ∈ [1e-3, 5e-4], `batch_size` ∈ [16, 32]
*   **Tiêu chí chọn Best Model:** Root Mean Square Error (RMSE) thấp nhất trên tập Validation.

#### [MODIFY] [p1_revenue/train.py](file:///d:/HK2_Nam4/KLTN/Final-Project/tlcn-lakehouse/src/mlops/p1_revenue/train.py)
- Thay đổi chữ ký hàm `train_lightgbm` và `train_lstm` để nhận các tha số động.
- Giới thiệu danh sách tham số `from sklearn.model_selection import ParameterGrid`.
- Xây dựng vòng lặp `for config in ParameterGrid(...)`. Sử dụng `mlflow.start_run(nested=True)` cho các lần chạy thử nghiệm từng mô hình.
- So sánh RMSE của từng cấu hình, và sau cùng mới thực hiện `mlflow.lightgbm.log_model(..., registered_model_name="revenue_lightgbm")`.

---

### 2. P2 - Anomaly Detection (Autoencoder)
*   **Grid Autoencoder:** `lr` ∈ [1e-3, 5e-4], `batch_size` ∈ [128, 256]
*   **Tiêu chí chọn Best Model:** Train Loss thấp nhất sau Epoch cuối (vì học không giám sát, việc nén/khôi phục tốt nhất đồng nghĩa với Loss nhượng bộ nhỏ).

#### [MODIFY] [p2_anomaly/train.py](file:///d:/HK2_Nam4/KLTN/Final-Project/tlcn-lakehouse/src/mlops/p2_anomaly/train.py)
- Chuyển `LR` và `BATCH_SIZE` thành các tham số nội bộ thay vì biến toàn cục.
- Bọc toàn bộ pipeline train trong vòng lặp Grid Search.
- Gắn Log Nested MLflow và chỉ Registry phiên bản có Final Loss tốt nhất.

---

### 3. P3 - Late Delivery Prediction (LSTM Classifier)
*   **Grid LSTM Classifier:** `lr` ∈ [1e-3, 5e-4], `batch_size` ∈ [32, 64]
*   **Tiêu chí chọn Best Model:** F1 Score cao nhất trên tập Validation.

#### [MODIFY] [p3_late_delivery/train.py](file:///d:/HK2_Nam4/KLTN/Final-Project/tlcn-lakehouse/src/mlops/p3_late_delivery/train.py)
- Tương tự P2, nhét Grid Search cho LSTM với `nested=True` trên MLFlow.
- Viết luận lý lưu cấu hình cho F1 Score cao nhất làm Champion Model. Cấu hình này sẽ được Register thành `late_delivery_lstm`.

## Open Questions

> [!NOTE]
> 1. Hiện tại ở đoạn cuối script P1, code đang có lệnh tự động print ra "Winner" giữa `LightGBM` và `LSTM` dựa trên cấu hình tốt nhất của chúng. Tôi sẽ giữ lại logic này, nhưng bổ sung thêm để thông báo cụ thể là "Winner thuộc về mô hình nào kèm siêu tham số (Hyperparameters) nào", bạn đồng ý chứ?
> 2. Quá trình train sẽ mất thêm vài phút (do nhân rộng thời gian chạy x4, x6). Bạn có muốn duy trì Early Stopping hay tinh chỉnh gì về độ sâu vòng lặp không?

## Verification Plan
### Manual Verification
- Chạy thử `python -m src.mlops.p1_revenue.train`.
- Truy cập `http://localhost:5000` (MLflow UI), đi vào Experiment `P1_Revenue_Forecasting`. Trực quan hóa các run theo dạng lồng nhau (Parent -> Child runs) để xác nhận tất cả cấu hình đều được log.
- Xác nhận chỉ có 1 mô hình `revenue_lightgbm` best được đăng ký trên Model Registry trong 1 lần train.
