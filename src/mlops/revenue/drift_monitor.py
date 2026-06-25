from __future__ import annotations

import logging
import numpy as np
import pandas as pd

from src.mlops.data_loader import load_revenue_data
from src.mlops.revenue.features import TRAIN_RATIO, TARGET_COL

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def calculate_psi(expected: pd.Series, actual: pd.Series, num_buckets: int = 10) -> float:
    breakpoints = np.percentile(expected, np.linspace(0, 100, num_buckets + 1))
    breakpoints[0] -= 1e-5
    breakpoints[-1] += 1e-5

    expected_counts = pd.cut(expected, bins=breakpoints).value_counts(sort=False)
    actual_counts = pd.cut(actual, bins=breakpoints).value_counts(sort=False)

    expected_pct = expected_counts / len(expected)
    actual_pct = actual_counts / len(actual)

    # Tránh chia cho 0
    expected_pct = np.where(expected_pct == 0, 0.0001, expected_pct)
    actual_pct = np.where(actual_pct == 0, 0.0001, actual_pct)

    psi = np.sum((actual_pct - expected_pct) * np.log(actual_pct / expected_pct))
    return float(psi)


def check_revenue_drift():
    logger.info("=" * 80)
    logger.info(" HỆ THỐNG GIÁM SÁT MLOPS — DETECT DATA DRIFT (PSI & Z-SCORE) ".center(80, "="))
    logger.info("=" * 80)

    df = load_revenue_data().sort_values(["year", "week_of_year"]).reset_index(drop=True)
    if len(df) < 10:
        logger.warning("Không đủ dữ liệu để tính toán Drift.")
        return False

    split = int(len(df) * TRAIN_RATIO)
    base_data = df[TARGET_COL][:split]
    recent_data = df[TARGET_COL][split:]

    logger.info("➜ Dữ liệu cơ sở (Baseline - Training): %d tuần", len(base_data))
    logger.info("➜ Dữ liệu gần đây (Recent - Inference): %d tuần", len(recent_data))

    mean_base = base_data.mean()
    std_base = base_data.std()
    mean_recent = recent_data.mean()
    
    z_score = abs(mean_recent - mean_base) / (std_base / np.sqrt(len(recent_data)))
    logger.info("\n1. ĐÁNH GIÁ Z-SCORE (Kiểm định giả thuyết sự thay đổi trung bình):")
    logger.info("   - Doanh thu tuần TB (Baseline): {:,.2f}".format(mean_base))
    logger.info("   - Doanh thu tuần TB (Gần đây):  {:,.2f}".format(mean_recent))
    logger.info("   ➜ Z-Score = %.3f", z_score)

    psi_score = calculate_psi(base_data, recent_data, num_buckets=5)
    logger.info("\n2. ĐÁNH GIÁ PSI (Population Stability Index):")
    logger.info("   ➜ PSI Score = %.4f", psi_score)

    logger.info("\n--- KẾT LUẬN & ĐÁNH GIÁ MLOPS ---")
    drift_detected = False
    if psi_score >= 0.25:
        logger.warning("❌ CẢNH BÁO: Phát hiện Data Drift nghiêm trọng (PSI >= 0.25)!")
        drift_detected = True
    elif psi_score >= 0.1:
        logger.warning("⚠️ CẢNH BÁO: Phân phối bắt đầu thay đổi (0.1 <= PSI < 0.25). Cần theo dõi thêm.")
    else:
        logger.info("✅ ỔN ĐỊNH: Không phát hiện Data Drift (PSI < 0.1). Mô hình hiện tại vẫn chính xác.")

    if z_score > 2.58:
        logger.warning("⚠️ CẢNH BÁO: Trị giá trung bình doanh thu đã biến động đáng kể (Z-Score > 2.58).")

    if drift_detected:
        logger.info("➜ ĐỀ XUẤT: Kích hoạt luồng Retrain (Huấn luyện lại) mô hình LightGBM tự động.")
    else:
        logger.info("➜ ĐỀ XUẤT: Giữ nguyên mô hình Champion hiện tại trên MLflow.")

    return drift_detected


if __name__ == "__main__":
    check_revenue_drift()
