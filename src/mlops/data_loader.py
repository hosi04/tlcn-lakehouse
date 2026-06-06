import logging
import pandas as pd
from pyspark.sql import SparkSession
from src.etl.utils import get_spark_session

logger = logging.getLogger(__name__)


def load_revenue_data(spark: SparkSession = None) -> pd.DataFrame:
    _owns_spark = spark is None
    if _owns_spark:
        spark = get_spark_session("MLOps-Revenue")
    logger.info("[data_loader] Loading feature-rich weekly revenue data...")
    try:
        df = spark.sql("""
        WITH weekly_base AS (
            SELECT
                d.year,
                d.week_of_year,
                MIN(d.month)                                        AS month,
                SUM(fo.total_payment_value)                         AS weekly_revenue,
                COUNT(DISTINCT fo.order_key)                        AS order_count,
                SUM(fo.total_product_value)                         AS total_product_value,
                SUM(fo.total_freight_value)                         AS total_freight_value,
                SUM(fo.total_payment_value)
                    / NULLIF(COUNT(DISTINCT fo.order_key), 0)       AS avg_order_value,
                SUM(fo.number_of_items)
                    / NULLIF(COUNT(DISTINCT fo.order_key), 0)       AS avg_items_per_order,
                SUM(fo.total_freight_value)
                    / NULLIF(SUM(fo.total_payment_value), 0)        AS freight_revenue_ratio,
                SUM(fo.total_freight_value)
                    / NULLIF(SUM(fo.number_of_items), 0)            AS avg_freight_per_item,
                COUNT(DISTINCT fo.customer_key)                     AS active_customers_count,
                AVG(fo.delivery_actual_days)                        AS avg_delivery_days,
                AVG(fo.delivery_estimate_days)                      AS avg_estimate_days,
                SUM(CASE WHEN fo.delivery_early_days < 0 THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(DISTINCT fo.order_key), 0)       AS late_delivery_rate,
                SUM(CASE WHEN fo.primary_payment_type = 'credit_card' THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(DISTINCT fo.order_key), 0)       AS credit_card_ratio,
                AVG(fo.total_installments)                          AS avg_installments,
                SUM(CASE WHEN d.is_weekend THEN 1 ELSE 0 END)
                    / NULLIF(COUNT(DISTINCT fo.order_key), 0)       AS weekend_order_ratio

            FROM iceberg.gold.fact_order fo
            JOIN iceberg.gold.dim_date d
                ON fo.purchase_date_key = d.date_key
            WHERE fo.order_status = 'delivered'
            GROUP BY d.year, d.week_of_year
        ),

        weekly_sellers AS (
            SELECT
                d.year,
                d.week_of_year,
                COUNT(DISTINCT foi.seller_key)                      AS active_sellers_count
            FROM iceberg.gold.fact_order_item foi
            JOIN iceberg.gold.dim_date d
                ON foi.purchase_date_key = d.date_key
            WHERE foi.order_status = 'delivered'
            GROUP BY d.year, d.week_of_year
        ),

        weekly_enriched AS (
            SELECT
                b.*,
                COALESCE(s.active_sellers_count, 0)                 AS active_sellers_count
            FROM weekly_base b
            LEFT JOIN weekly_sellers s
                ON b.year = s.year AND b.week_of_year = s.week_of_year
        ),

        weekly_lagged AS (
            SELECT
                *,

                LAG(weekly_revenue, 1) OVER (ORDER BY year, week_of_year) AS revenue_lag_1,
                LAG(weekly_revenue, 2) OVER (ORDER BY year, week_of_year) AS revenue_lag_2,
                LAG(weekly_revenue, 4) OVER (ORDER BY year, week_of_year) AS revenue_lag_4,

                LAG(order_count, 1) OVER (ORDER BY year, week_of_year)    AS order_count_lag_1,
                LAG(order_count, 2) OVER (ORDER BY year, week_of_year)    AS order_count_lag_2,
                LAG(order_count, 4) OVER (ORDER BY year, week_of_year)    AS order_count_lag_4,
                AVG(weekly_revenue) OVER (
                    ORDER BY year, week_of_year
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_4w,

                STDDEV(weekly_revenue) OVER (
                    ORDER BY year, week_of_year
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS rolling_std_4w,

                AVG(weekly_revenue) OVER (
                    ORDER BY year, week_of_year
                    ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING
                ) AS rolling_avg_8w,

                AVG(order_count) OVER (
                    ORDER BY year, week_of_year
                    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
                ) AS order_rolling_avg_4w

            FROM weekly_enriched
        )

        SELECT
            *,
            CASE
                WHEN revenue_lag_2 IS NOT NULL AND revenue_lag_2 > 0
                THEN (revenue_lag_1 - revenue_lag_2) / revenue_lag_2
                ELSE 0.0
            END AS revenue_momentum,

            CASE
                WHEN revenue_lag_2 IS NOT NULL AND revenue_lag_2 > 0
                     AND LAG(revenue_lag_1, 1) OVER (ORDER BY year, week_of_year) IS NOT NULL
                THEN (revenue_lag_1 - revenue_lag_2) / revenue_lag_2
                     - (revenue_lag_2 - revenue_lag_4) / NULLIF(revenue_lag_4, 0)
                ELSE 0.0
            END AS revenue_acceleration

        FROM weekly_lagged
        ORDER BY year, week_of_year
        """)

        pdf = df.toPandas()

        lag_rolling_cols = [
            "revenue_lag_1", "revenue_lag_2", "revenue_lag_4",
            "order_count_lag_1", "order_count_lag_2", "order_count_lag_4",
            "rolling_avg_4w", "rolling_std_4w", "rolling_avg_8w",
            "order_rolling_avg_4w",
            "revenue_momentum", "revenue_acceleration",
        ]
        for col in lag_rolling_cols:
            if col in pdf.columns:
                pdf[col] = pdf[col].fillna(0)

        logger.info(
            "[data_loader] Revenue data: %d weeks x %d features | "
            "date range: %d-W%d -> %d-W%d",
            len(pdf), len(pdf.columns),
            pdf["year"].min(), pdf["week_of_year"].min(),
            pdf["year"].max(), pdf["week_of_year"].max(),
        )
        return pdf
    finally:
        if _owns_spark:
            spark.stop()
