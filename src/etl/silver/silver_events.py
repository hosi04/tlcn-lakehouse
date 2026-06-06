
import logging
import os
from datetime import datetime
from typing import Dict, Optional, Tuple

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, desc, get_json_object, lit, max as spark_max, row_number, to_date
from pyspark.sql.window import Window

from src.etl.silver.silver_assets import read_from_iceberg
from src.etl.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_TABLE = "olist_events"
SILVER_TABLE = "events"
SILVER_NAMESPACE = "iceberg.silver"
FULL_SILVER_TABLE = f"{SILVER_NAMESPACE}.{SILVER_TABLE}"
WATERMARK_TABLE = "pipeline_watermarks"
PIPELINE_NAME = "events_bronze_to_silver"

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

SILVER_COLUMNS = [
    "event_id",
    "event_type",
    "event_time",
    "event_date",
    "user_id",
    "platform",
    "source_channel",
    "device_type",
    "context_path",
    "keyword",
    "result_count",
    "category_id",
    "category_name",
    "product_id",
    "product_name",
    "price",
    "quantity",
    "currency",
    "cart_id",
    "order_id",
    "total_amount",
    "discount",
    "revenue",
    "payment_method",
    "item_count",
    "properties_json",
    "ingested_at",
]


def _json(path: str):
    return get_json_object(col("properties_json"), path)


def _transform_bronze(df: DataFrame) -> DataFrame:
    df = df.dropDuplicates(["event_id", "kafka_partition", "kafka_offset"])
    df = df.filter(col("event_type").isin(list(VALID_EVENT_TYPES)))
    df = df.filter(col("context_platform").isin(list(VALID_PLATFORMS)))
    df = df.na.drop(subset=["event_id", "event_type", "event_time", "user_id"])

    dedupe_window = Window.partitionBy("event_id").orderBy(desc("ingested_at"))
    df = (
        df.withColumn("_row_num", row_number().over(dedupe_window))
        .filter(col("_row_num") == 1)
        .drop("_row_num")
    )

    return (
        df.withColumn("event_date", to_date(col("event_time")))
        .withColumn("platform", col("context_platform"))
        .withColumn("source_channel", col("context_source_channel"))
        .withColumn("device_type", col("context_device_type"))
        .withColumn("context_path", col("context_path"))
        .withColumn("keyword", _json("$.keyword"))
        .withColumn("result_count", _json("$.result_count").cast("int"))
        .withColumn("category_id", _json("$.category_id"))
        .withColumn("category_name", _json("$.category_name"))
        .withColumn("product_id", _json("$.product_id"))
        .withColumn("product_name", _json("$.product_name"))
        .withColumn("price", _json("$.price").cast("double"))
        .withColumn("quantity", _json("$.quantity").cast("int"))
        .withColumn("currency", _json("$.currency"))
        .withColumn("cart_id", _json("$.cart_id"))
        .withColumn("order_id", _json("$.order_id"))
        .withColumn("total_amount", _json("$.total_amount").cast("double"))
        .withColumn("discount", _json("$.discount").cast("double"))
        .withColumn("revenue", _json("$.revenue").cast("double"))
        .withColumn("payment_method", _json("$.payment_method"))
        .withColumn("item_count", _json("$.item_count").cast("int"))
        .select(*SILVER_COLUMNS)
    )


def _ensure_watermark_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {SILVER_NAMESPACE}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {SILVER_NAMESPACE}.{WATERMARK_TABLE} (
            pipeline_name STRING,
            watermark TIMESTAMP,
            updated_at TIMESTAMP
        )
        USING iceberg
        """
    )


def _read_watermark(spark: SparkSession) -> Optional[datetime]:
    _ensure_watermark_table(spark)
    rows = (
        spark.table(f"{SILVER_NAMESPACE}.{WATERMARK_TABLE}")
        .filter(col("pipeline_name") == PIPELINE_NAME)
        .select("watermark")
        .collect()
    )
    return rows[0]["watermark"] if rows else None


def _write_watermark(spark: SparkSession, watermark: datetime) -> None:
    update = spark.createDataFrame(
        [(PIPELINE_NAME, watermark)],
        "pipeline_name string, watermark timestamp",
    ).withColumn("updated_at", current_timestamp())
    update.createOrReplaceTempView("watermark_update")
    spark.sql(
        f"""
        MERGE INTO {SILVER_NAMESPACE}.{WATERMARK_TABLE} t
        USING watermark_update s
        ON t.pipeline_name = s.pipeline_name
        WHEN MATCHED THEN UPDATE SET
            watermark = s.watermark,
            updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def _read_bronze_incremental(spark: SparkSession, watermark: Optional[datetime]) -> DataFrame:
    df = read_from_iceberg(spark, BRONZE_TABLE)
    if watermark is not None:
        logger.info("Reading bronze rows with ingested_at > %s", watermark)
        df = df.filter(col("ingested_at") > watermark)
    else:
        logger.info("No watermark found — reading all bronze rows (initial load)")
    return df


def _merge_into_silver(spark: SparkSession, updates: DataFrame) -> None:
    updates.createOrReplaceTempView("events_updates")
    update_set = ", ".join(f"{column} = s.{column}" for column in SILVER_COLUMNS if column != "event_id")
    insert_cols = ", ".join(SILVER_COLUMNS)
    insert_vals = ", ".join(f"s.{column}" for column in SILVER_COLUMNS)

    spark.sql(
        f"""
        MERGE INTO {FULL_SILVER_TABLE} t
        USING events_updates s
        ON t.event_id = s.event_id
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def _write_initial_silver(spark: SparkSession, df: DataFrame) -> None:
    df.write.format("iceberg").mode("overwrite").saveAsTable(FULL_SILVER_TABLE)


def silver_cleaned_events(spark: SparkSession) -> Tuple[DataFrame, Dict]:
    mode = os.getenv("SILVER_EVENTS_MODE", "incremental").lower()
    full_refresh = mode == "full"

    if full_refresh:
        logger.info("SILVER_EVENTS_MODE=full — rebuilding silver.events from all bronze")
        bronze_df = read_from_iceberg(spark, BRONZE_TABLE)
        watermark = None
    else:
        watermark = _read_watermark(spark)
        bronze_df = _read_bronze_incremental(spark, watermark)

    if bronze_df.isEmpty():
        silver_count = spark.table(FULL_SILVER_TABLE).count() if spark.catalog.tableExists(FULL_SILVER_TABLE) else 0
        logger.info("No new bronze rows — silver unchanged (%s rows)", silver_count)
        return bronze_df, {
            "mode": mode,
            "processed_rows": 0,
            "silver_row_count": silver_count,
            "watermark": watermark,
        }

    silver_df = _transform_bronze(bronze_df)
    processed_rows = silver_df.count()
    batch_watermark = bronze_df.agg(spark_max("ingested_at")).collect()[0][0]

    if full_refresh or not spark.catalog.tableExists(FULL_SILVER_TABLE):
        _write_initial_silver(spark, silver_df)
        logger.info("Created %s with %s rows", FULL_SILVER_TABLE, processed_rows)
    else:
        _merge_into_silver(spark, silver_df)
        logger.info("Merged %s rows into %s", processed_rows, FULL_SILVER_TABLE)

    if batch_watermark is not None:
        _write_watermark(spark, batch_watermark)

    silver_count = spark.table(FULL_SILVER_TABLE).count()
    metadata = {
        "mode": mode,
        "processed_rows": processed_rows,
        "silver_row_count": silver_count,
        "watermark": batch_watermark,
    }
    logger.info("Silver events sync done: %s", metadata)
    return silver_df, metadata


def main() -> None:
    load_dotenv()
    spark = get_spark_session("Silver Events")

    try:
        silver_cleaned_events(spark)
        logger.info("Silver events processing completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
