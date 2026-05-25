import logging
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    get_json_object,
    to_timestamp,
)
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.spark.bronze.bronze_assets import create_bucket_if_not_exists
from src.spark.utils import get_spark_session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "olist-events")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
BRONZE_TABLE = os.getenv("KAFKA_BRONZE_TABLE", "olist_events")
BRONZE_NAMESPACE = os.getenv("KAFKA_BRONZE_NAMESPACE", "iceberg.bronze")
CHECKPOINT_LOCATION = os.getenv(
    "KAFKA_CHECKPOINT_LOCATION",
    "s3a://lakehouse/checkpoints/bronze-olist-events",
)
TRIGGER_INTERVAL = os.getenv("KAFKA_STREAM_TRIGGER", "30 seconds")
MAX_OFFSETS_PER_TRIGGER = os.getenv("KAFKA_MAX_OFFSETS_PER_TRIGGER", "5000")

FULL_TABLE_NAME = f"{BRONZE_NAMESPACE}.{BRONZE_TABLE}"

CONTEXT_SCHEMA = StructType(
    [
        StructField("url", StringType()),
        StructField("path", StringType()),
        StructField("title", StringType()),
        StructField("platform", StringType()),
        StructField("source_channel", StringType()),
        StructField("device_type", StringType()),
    ]
)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("event_time", StringType()),
        StructField("user_id", StringType()),
        StructField("context", CONTEXT_SCHEMA),
    ]
)


def kafka_to_bronze_df(kafka_df: DataFrame) -> DataFrame:
    payload = col("value").cast("string")

    parsed = kafka_df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        payload.alias("raw_event"),
        from_json(payload, EVENT_SCHEMA).alias("e"),
        get_json_object(payload, "$.properties").alias("properties_json"),
    )

    return parsed.select(
        col("e.event_id"),
        col("e.event_type"),
        to_timestamp(col("e.event_time"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("event_time"),
        col("e.user_id"),
        col("properties_json"),
        col("e.context.url").alias("context_url"),
        col("e.context.path").alias("context_path"),
        col("e.context.title").alias("context_title"),
        col("e.context.platform").alias("context_platform"),
        col("e.context.source_channel").alias("context_source_channel"),
        col("e.context.device_type").alias("context_device_type"),
        col("kafka_topic"),
        col("kafka_partition").cast(IntegerType()),
        col("kafka_offset").cast(LongType()),
        col("kafka_timestamp"),
        col("raw_event"),
        current_timestamp().alias("ingested_at"),
    )


def _write_micro_batch(spark: SparkSession, bronze_df: DataFrame, epoch_id: int, table_ready: list[bool]) -> None:
    if bronze_df.isEmpty():
        logger.debug("Epoch %s: no new Kafka messages", epoch_id)
        return

    row_count = bronze_df.count()
    if not table_ready[0]:
        bronze_df.write.format("iceberg").mode("overwrite").saveAsTable(FULL_TABLE_NAME)
        table_ready[0] = True
        logger.info("Created %s with %s rows (epoch %s)", FULL_TABLE_NAME, row_count, epoch_id)
        return

    bronze_df.write.format("iceberg").mode("append").saveAsTable(FULL_TABLE_NAME)
    logger.info("Appended %s rows to %s (epoch %s)", row_count, FULL_TABLE_NAME, epoch_id)


def start_kafka_stream(spark: SparkSession):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {BRONZE_NAMESPACE}")

    table_ready = [spark.catalog.tableExists(FULL_TABLE_NAME)]

    def foreach_batch(kafka_df: DataFrame, epoch_id: int) -> None:
        bronze_df = kafka_to_bronze_df(kafka_df)
        _write_micro_batch(spark, bronze_df, epoch_id, table_ready)

    logger.info(
        "Starting stream: topic=%s bootstrap=%s startingOffsets=%s trigger=%s checkpoint=%s",
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_STARTING_OFFSETS,
        TRIGGER_INTERVAL,
        CHECKPOINT_LOCATION,
    )

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)
        .load()
    )

    return (
        kafka_stream.writeStream.foreachBatch(foreach_batch)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def main() -> None:
    load_dotenv()
    create_bucket_if_not_exists("lakehouse")

    spark = get_spark_session("Bronze Kafka Stream")
    logger.info("Bronze streaming ingest started at %s", datetime.now(timezone.utc).isoformat())
    logger.info("Run generator in another terminal: python -m src.streaming.generator")

    query = start_kafka_stream(spark)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Shutting down stream (Ctrl+C)...")
    finally:
        query.stop()
        spark.stop()
        logger.info("Spark session stopped.")


if __name__ == "__main__":
    main()
