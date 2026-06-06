import logging
from typing import Dict, Tuple

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    explode,
    first as spark_first,
    from_json,
    get_json_object,
    lit,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

from src.etl.gold.gold_assets import read_from_iceberg, write_to_iceberg
from src.etl.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_TABLE = "events"

PURCHASE_ITEM_SCHEMA = ArrayType(
    StructType(
        [
            StructField("product_id", StringType()),
            StructField("quantity", IntegerType()),
            StructField("price", DoubleType()),
        ]
    )
)


def _read_events(spark: SparkSession) -> DataFrame:
    return read_from_iceberg(spark, SILVER_TABLE)


def gold_agg_daily_funnel(spark: SparkSession) -> Tuple[DataFrame, Dict]:
    df = _read_events(spark)

    agg = df.groupBy("event_date", "platform", "source_channel").agg(
        count(when(col("event_type") == "search", 1)).alias("search_count"),
        count(when(col("event_type") == "view_category", 1)).alias("view_category_count"),
        count(when(col("event_type") == "view_item", 1)).alias("view_item_count"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        count(when(col("event_type") == "remove_from_cart", 1)).alias("remove_from_cart_count"),
        count(when(col("event_type") == "begin_checkout", 1)).alias("begin_checkout_count"),
        count(when(col("event_type") == "purchase", 1)).alias("purchase_count"),
        countDistinct(when(col("event_type") == "search", col("user_id"))).alias("unique_users_search"),
        countDistinct(when(col("event_type") == "view_item", col("user_id"))).alias("unique_users_view_item"),
        countDistinct(when(col("event_type") == "add_to_cart", col("user_id"))).alias("unique_users_add_to_cart"),
        countDistinct(when(col("event_type") == "purchase", col("user_id"))).alias("unique_users_purchase"),
        spark_sum(when(col("event_type") == "purchase", col("revenue")).otherwise(lit(0.0))).alias("total_revenue"),
    )

    agg = agg.withColumn(
        "conversion_rate_view_to_purchase",
        when(
            col("unique_users_view_item") > 0,
            col("unique_users_purchase") / col("unique_users_view_item"),
        ).otherwise(lit(0.0)),
    ).withColumn(
        "conversion_rate_cart_to_purchase",
        when(
            col("unique_users_add_to_cart") > 0,
            col("unique_users_purchase") / col("unique_users_add_to_cart"),
        ).otherwise(lit(0.0)),
    )

    metadata = write_to_iceberg(spark, agg, "agg_daily_funnel")
    logger.info("Gold agg_daily_funnel: %s rows", metadata["row_count"])
    return agg, metadata


def gold_agg_product_engagement(spark: SparkSession) -> Tuple[DataFrame, Dict]:
    df = _read_events(spark)

    direct = df.filter(col("product_id").isNotNull()).select(
        col("event_date"),
        col("product_id"),
        col("product_name"),
        col("category_id"),
        col("category_name"),
        col("event_type"),
        col("user_id"),
        col("price"),
        lit(0.0).alias("line_revenue"),
    )

    purchase_lines = (
        df.filter(col("event_type") == "purchase")
        .select(
            col("event_date"),
            col("user_id"),
            explode(
                from_json(get_json_object(col("properties_json"), "$.items"), PURCHASE_ITEM_SCHEMA)
            ).alias("item"),
        )
        .select(
            col("event_date"),
            col("item.product_id").alias("product_id"),
            lit(None).cast("string").alias("product_name"),
            lit(None).cast("string").alias("category_id"),
            lit(None).cast("string").alias("category_name"),
            lit("purchase").alias("event_type"),
            col("user_id"),
            col("item.price").alias("price"),
            (col("item.price") * col("item.quantity")).alias("line_revenue"),
        )
    )

    combined = direct.unionByName(purchase_lines)

    product_dim = (
        df.filter(col("product_id").isNotNull())
        .groupBy("product_id")
        .agg(
            spark_first("product_name", ignorenulls=True).alias("product_name_ref"),
            spark_first("category_id", ignorenulls=True).alias("category_id_ref"),
            spark_first("category_name", ignorenulls=True).alias("category_name_ref"),
        )
    )

    combined = combined.join(product_dim, on="product_id", how="left")
    combined = combined.withColumn(
        "product_name",
        when(col("product_name").isNull(), col("product_name_ref")).otherwise(col("product_name")),
    ).withColumn(
        "category_id",
        when(col("category_id").isNull(), col("category_id_ref")).otherwise(col("category_id")),
    ).withColumn(
        "category_name",
        when(col("category_name").isNull(), col("category_name_ref")).otherwise(col("category_name")),
    ).drop("product_name_ref", "category_id_ref", "category_name_ref")

    agg = combined.groupBy("event_date", "product_id", "product_name", "category_id", "category_name").agg(
        count(when(col("event_type") == "view_item", 1)).alias("view_count"),
        count(when(col("event_type") == "add_to_cart", 1)).alias("add_to_cart_count"),
        count(when(col("event_type") == "remove_from_cart", 1)).alias("remove_from_cart_count"),
        count(when(col("event_type") == "purchase", 1)).alias("purchase_line_count"),
        countDistinct(when(col("event_type") == "view_item", col("user_id"))).alias("unique_viewers"),
        spark_sum(when(col("event_type") == "purchase", col("line_revenue")).otherwise(lit(0.0))).alias(
            "attributed_revenue"
        ),
    )

    metadata = write_to_iceberg(spark, agg, "agg_product_engagement")
    logger.info("Gold agg_product_engagement: %s rows", metadata["row_count"])
    return agg, metadata


def gold_agg_search_keywords(spark: SparkSession) -> Tuple[DataFrame, Dict]:
    df = _read_events(spark).filter(
        (col("event_type") == "search") & col("keyword").isNotNull()
    )

    agg = df.groupBy("event_date", "keyword").agg(
        count(lit(1)).alias("search_count"),
        countDistinct("user_id").alias("unique_searchers"),
        avg("result_count").alias("avg_result_count"),
    )

    metadata = write_to_iceberg(spark, agg, "agg_search_keywords")
    logger.info("Gold agg_search_keywords: %s rows", metadata["row_count"])
    return agg, metadata


def main() -> None:
    load_dotenv()
    spark = get_spark_session("Gold Events")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    try:
        gold_agg_daily_funnel(spark)
        gold_agg_product_engagement(spark)
        gold_agg_search_keywords(spark)
        logger.info("Gold events processing completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
