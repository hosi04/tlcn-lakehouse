import argparse
import json
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer

from src.streaming.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ORDER_EVENTS_TOPIC,
    STREAM_DATA_ROOT,
)


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _clean_value(value):
    if pd.isna(value):
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def replay_order_events(
    input_path: Path,
    bootstrap_servers: str,
    topic: str,
    rate: float,
    limit: int | None,
) -> None:
    if rate <= 0:
        raise ValueError("rate must be greater than 0")

    df = pd.read_csv(input_path)
    if limit is not None:
        df = df.head(limit)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        linger_ms=10,
    )

    delay_seconds = 1.0 / rate
    logger.info(
        "Replaying %s events to topic=%s bootstrap=%s rate=%s/s",
        len(df),
        topic,
        bootstrap_servers,
        rate,
    )

    try:
        for idx, row in enumerate(df.to_dict(orient="records"), 1):
            event = {key: _clean_value(value) for key, value in row.items()}
            producer.send(topic, key=str(event["order_id"]), value=event)

            if idx % 100 == 0 or idx == len(df):
                logger.info("Sent %s/%s events", idx, len(df))

            time.sleep(delay_seconds)
    finally:
        producer.flush()
        producer.close()

    logger.info("Replay completed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay order events to Kafka.")
    parser.add_argument(
        "--input-path",
        type=Path,
        default=STREAM_DATA_ROOT / "order_events_replay.csv",
    )
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=ORDER_EVENTS_TOPIC)
    parser.add_argument("--rate", type=float, default=5.0, help="Events per second.")
    parser.add_argument("--limit", type=int, default=None)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    replay_order_events(
        input_path=args.input_path,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rate=args.rate,
        limit=args.limit,
    )
