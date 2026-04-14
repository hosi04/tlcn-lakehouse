import logging
import pandas as pd
from src.spark.utils import get_spark_session

logger = logging.getLogger(__name__)


def load_revenue_data() -> pd.DataFrame:
    logger.info("[data_loader] Loading weekly revenue data...")
    spark = get_spark_session("MLOps-Revenue")
    try:
        df = spark.sql("""
            SELECT
                d.year,
                d.week_of_year,
                d.month,
                CAST(d.is_weekend AS INT)        AS is_weekend,
                SUM(fo.total_payment_value)      AS weekly_revenue,
                COUNT(fo.order_key)              AS order_count
            FROM iceberg.gold.fact_order fo
            JOIN iceberg.gold.dim_date d
                ON fo.purchase_date_key = d.date_key
            WHERE fo.order_status = 'delivered'
            GROUP BY d.year, d.week_of_year, d.month, d.is_weekend
            ORDER BY d.year, d.week_of_year
        """)
        pdf = df.toPandas()
        logger.info(f"[data_loader] Revenue data: {len(pdf)} weeks")
        return pdf
    finally:
        spark.stop()


def load_anomaly_data() -> pd.DataFrame:
    logger.info("[data_loader] Loading anomaly detection data...")
    spark = get_spark_session("MLOps-Anomaly")
    try:
        df = spark.sql("""
            SELECT
                delivery_actual_days,
                delivery_estimate_days,
                total_freight_value,
                total_product_value,
                number_of_items
            FROM iceberg.gold.fact_order
            WHERE order_status         = 'delivered'
              AND delivery_actual_days IS NOT NULL
              AND delivery_estimate_days IS NOT NULL
              AND total_freight_value  > 0
              AND number_of_items      > 0
        """)
        pdf = df.toPandas()
        logger.info(f"[data_loader] Anomaly data: {len(pdf)} orders")
        return pdf
    finally:
        spark.stop()


def load_late_delivery_data(min_weeks: int = 10) -> pd.DataFrame:
    logger.info("[data_loader] Loading late delivery data...")
    spark = get_spark_session("MLOps-LateDelivery")
    try:
        df = spark.sql("""
            SELECT
                foi.seller_key,
                d.year,
                d.week_of_year,
                AVG(fo.delivery_actual_days)                              AS avg_delivery_days,
                AVG(fo.delivery_estimate_days)                            AS avg_estimate_days,
                COUNT(fo.order_key)                                       AS order_count,
                SUM(CASE WHEN fo.delivery_early_days < 0 THEN 1 ELSE 0 END)
                    / COUNT(fo.order_key)                                 AS late_rate,
                CAST(
                    MAX(CASE WHEN fo.delivery_early_days < 0 THEN 1 ELSE 0 END)
                AS INT)                                                   AS is_late_week
            FROM iceberg.gold.fact_order_item foi
            JOIN iceberg.gold.fact_order fo
                ON foi.order_id = fo.order_id
            JOIN iceberg.gold.dim_date d
                ON fo.purchase_date_key = d.date_key
            WHERE fo.order_status          = 'delivered'
              AND fo.delivery_early_days  IS NOT NULL
            GROUP BY foi.seller_key, d.year, d.week_of_year
            ORDER BY foi.seller_key, d.year, d.week_of_year
        """)
        pdf = df.toPandas()

        week_counts = pdf.groupby("seller_key")["week_of_year"].count()
        valid_sellers = week_counts[week_counts >= min_weeks].index
        pdf = pdf[pdf["seller_key"].isin(valid_sellers)].reset_index(drop=True)

        logger.info(
            f"[data_loader] Late delivery data: {len(pdf)} seller-weeks "
            f"({pdf['seller_key'].nunique()} sellers, min_weeks≥{min_weeks})"
        )
        return pdf
    finally:
        spark.stop()
