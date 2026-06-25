from __future__ import annotations

import os
import logging
import trino
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

MAINTENANCE_TABLES = [
    "iceberg.gold.fact_order",
    "iceberg.gold.fact_order_item",
    "iceberg.gold.dim_customer",
    "iceberg.gold.dim_product",
    "iceberg.gold.dim_seller",
    "iceberg.silver.events",
    "iceberg.gold.agg_daily_funnel",
    "iceberg.gold.agg_product_engagement",
    "iceberg.gold.agg_search_keywords",
]


def run_maintenance():
    logger.info("=" * 80)
    logger.info(" BẢO TRÌ BẢNG ICEBERG ĐỊNH KỲ (COMPACTION & VACUUM) ".center(80, "="))
    logger.info("=" * 80)
    logger.info(" Kết nối Trino tại %s:%s", TRINO_HOST, TRINO_PORT)

    conn = trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )
    cur = conn.cursor()

    for table in MAINTENANCE_TABLES:
        logger.info("\n--- Đang xử lý bảng: %s ---", table)
        
        logger.info("  ➜ 1. Chạy Compaction (Gộp các file Parquet nhỏ)...")
        try:
            cur.execute(f"ALTER TABLE {table} EXECUTE OPTIMIZE")
            logger.info("     ✅ Compaction hoàn tất thành công.")
        except Exception as e:
            logger.warning("     ⚠️ Không thể chạy OPTIMIZE (bảng có thể đã tối ưu hoặc lỗi kết nối): %s", e)

        logger.info("  ➜ 2. Chạy Vacuum (Dọn dẹp snapshot cũ quá 7 ngày)...")
        try:
            cur.execute(f"ALTER TABLE {table} EXECUTE EXPIRE_SNAPSHOTS(retention_threshold => '7d')")
            logger.info("     ✅ Vacuum hoàn tất thành công.")
        except Exception as e:
            logger.warning("     ⚠️ Không thể chạy EXPIRE_SNAPSHOTS: %s", e)

    cur.close()
    conn.close()
    logger.info("\n✅ Đã hoàn tất bảo trì hệ thống Iceberg Lakehouse.")
    return True


if __name__ == "__main__":
    run_maintenance()
