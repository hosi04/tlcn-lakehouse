import argparse
import logging
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql.functions import col, current_timestamp, get_json_object

from src.spark.utils import get_spark_session
from src.streaming.config import (
    BRONZE_ORDER_EVENTS_TABLE,
    KAFKA_BOOTSTRAP_SERVERS,
    ORDER_EVENTS_TOPIC,
    STREAM_CHECKPOINT_ROOT,
)


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def start_bronze_order_events_stream(
    bootstrap_servers: str,
    topic: str,
    table_name: str,
    checkpoint_location: Path,
    starting_offsets: str,
    processing_time: str,
) -> None:
    load_dotenv()
    spark = get_spark_session("Bronze Order Events Stream")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    logger.info(
        "Starting Kafka -> Iceberg stream: topic=%s table=%s checkpoint=%s",
        topic,
        table_name,
        checkpoint_location,
    )

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    event_json = col("value").cast("string")
    bronze_df = kafka_df.select(
        get_json_object(event_json, "$.event_id").alias("event_id"),
        get_json_object(event_json, "$.event_type").alias("event_type"),
        get_json_object(event_json, "$.event_time").alias("event_time"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        event_json.alias("event_json"),
        current_timestamp().alias("ingested_at"),
    )

    query = (
        bronze_df.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", str(checkpoint_location))
        .trigger(processingTime=processing_time)
        .toTable(table_name)
    )

    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream Kafka order events into a Bronze Iceberg table."
    )
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=ORDER_EVENTS_TOPIC)
    parser.add_argument("--table", default=BRONZE_ORDER_EVENTS_TABLE)
    parser.add_argument(
        "--checkpoint-location",
        type=Path,
        default=Path(STREAM_CHECKPOINT_ROOT) / "bronze_order_events_stream",
    )
    parser.add_argument("--starting-offsets", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--processing-time", default="10 seconds")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start_bronze_order_events_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        table_name=args.table,
        checkpoint_location=args.checkpoint_location,
        starting_offsets=args.starting_offsets,
        processing_time=args.processing_time,
    )
