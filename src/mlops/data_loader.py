import logging
import pandas as pd
from pyspark.sql import SparkSession
from src.spark.utils import get_spark_session

logger = logging.getLogger(__name__)


def load_revenue_data(spark: SparkSession = None) -> pd.DataFrame:
    _owns_spark = spark is None
    if _owns_spark:
        spark = get_spark_session("MLOps-Revenue")
    logger.info("[data_loader] Loading weekly revenue data...")
    try:
        df = spark.sql("""
            SELECT
                d.year,
                d.week_of_year,
                MIN(d.month)                     AS month,
                SUM(fo.total_payment_value)      AS weekly_revenue,
                COUNT(DISTINCT fo.order_key)     AS order_count,
                SUM(CASE WHEN d.is_weekend THEN 1 ELSE 0 END)
                    / COUNT(DISTINCT fo.order_key) AS weekend_order_ratio
            FROM iceberg.gold.fact_order fo
            JOIN iceberg.gold.dim_date d
                ON fo.purchase_date_key = d.date_key
            WHERE fo.order_status = 'delivered'
            GROUP BY d.year, d.week_of_year
            ORDER BY d.year, d.week_of_year
        """)
        pdf = df.toPandas()
        logger.info(f"[data_loader] Revenue data: {len(pdf)} weeks")
        return pdf
    finally:
        if _owns_spark:
            spark.stop()


def load_anomaly_data(spark: SparkSession = None) -> pd.DataFrame:
    _owns_spark = spark is None
    if _owns_spark:
        spark = get_spark_session("MLOps-Anomaly")
    logger.info("[data_loader] Loading anomaly detection data...")
    try:
        df = spark.sql("""
            SELECT
                purchase_date_key,
                order_id,
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
            ORDER BY purchase_date_key, order_id
        """)
        pdf = df.toPandas()
        logger.info(f"[data_loader] Anomaly data: {len(pdf)} orders")
        return pdf
    finally:
        if _owns_spark:
            spark.stop()


def load_late_delivery_data(min_weeks: int = 10, spark: SparkSession = None) -> pd.DataFrame:
    _owns_spark = spark is None
    if _owns_spark:
        spark = get_spark_session("MLOps-LateDelivery")
    logger.info("[data_loader] Loading late delivery data...")
    try:
        df = spark.sql("""
            WITH seller_orders AS (
                SELECT DISTINCT
                    foi.seller_key,
                    fo.order_key,
                    d.year,
                    d.week_of_year,
                    fo.delivery_actual_days,
                    fo.delivery_estimate_days,
                    fo.delivery_early_days,
                    CASE WHEN fo.delivery_early_days < 0 THEN 1 ELSE 0 END AS is_late_order
                FROM iceberg.gold.fact_order_item foi
                JOIN iceberg.gold.fact_order fo
                    ON foi.order_key = fo.order_key
                JOIN iceberg.gold.dim_date d
                    ON fo.purchase_date_key = d.date_key
                WHERE fo.order_status         = 'delivered'
                  AND fo.delivery_early_days IS NOT NULL
            )
            SELECT
                seller_key,
                year,
                week_of_year,
                AVG(delivery_actual_days)                                 AS avg_delivery_days,
                AVG(delivery_estimate_days)                               AS avg_estimate_days,
                COUNT(DISTINCT order_key)                                 AS order_count,
                SUM(is_late_order) / COUNT(DISTINCT order_key)            AS late_rate,
                CAST(
                    MAX(is_late_order)
                AS INT)                                                   AS is_late_week
            FROM seller_orders
            GROUP BY seller_key, year, week_of_year
            ORDER BY seller_key, year, week_of_year
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
        if _owns_spark:
            spark.stop()
