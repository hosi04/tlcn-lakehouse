import logging
from typing import Dict, Tuple

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, get_json_object, to_date

from src.spark.silver.silver_assets import read_from_iceberg, write_to_iceberg
from src.spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE = "olist_events"
SILVER_TABLE = "events"

VALID_EVENT_TYPES = (
    "search",
    "view_category",
    "view_item",
    "add_to_cart",
    "remove_from_cart",
    "begin_checkout",
    "purchase",
)

VALID_PLATFORMS = ("web", "ios", "android")


def silver_cleaned_events(spark: SparkSession) -> Tuple[DataFrame, Dict]:
    df = read_from_iceberg(spark, BRONZE_TABLE)

    df = df.dropDuplicates(["event_id"])
    df = df.dropDuplicates(["kafka_partition", "kafka_offset"])

    df = df.filter(col("event_type").isin(list(VALID_EVENT_TYPES)))
    df = df.filter(col("context_platform").isin(list(VALID_PLATFORMS)))
    df = df.na.drop(subset=["event_id", "event_type", "event_time", "user_id"])

    df = (
        df.withColumn("event_date", to_date(col("event_time")))
        .withColumn("keyword", get_json_object(col("properties_json"), "$.keyword"))
        .withColumn("product_id", get_json_object(col("properties_json"), "$.product_id"))
        .withColumn("category_id", get_json_object(col("properties_json"), "$.category_id"))
        .withColumn("order_id", get_json_object(col("properties_json"), "$.order_id"))
        .withColumn(
            "revenue",
            get_json_object(col("properties_json"), "$.revenue").cast("double"),
        )
        .withColumn(
            "total_amount",
            get_json_object(col("properties_json"), "$.total_amount").cast("double"),
        )
        .drop("raw_event")
    )

    metadata = write_to_iceberg(spark, df, SILVER_TABLE)
    logger.info("Silver events: %s rows", metadata["row_count"])
    return df, metadata


def main() -> None:
    load_dotenv()
    spark = get_spark_session("Silver Events")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

    try:
        silver_cleaned_events(spark)
        logger.info("Silver events processing completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
