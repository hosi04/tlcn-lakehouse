import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_ROOT = Path(os.getenv("DATA_ROOT", PROJECT_ROOT / "data"))
BATCH_DATA_ROOT = Path(os.getenv("BATCH_DATA_ROOT", PROJECT_ROOT / "data" / "batch"))
STREAM_DATA_ROOT = Path(os.getenv("STREAM_DATA_ROOT", PROJECT_ROOT / "data" / "stream"))

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
ORDER_EVENTS_TOPIC = os.getenv("ORDER_EVENTS_TOPIC", "olist.order_events")

BRONZE_ORDER_EVENTS_TABLE = os.getenv(
    "BRONZE_ORDER_EVENTS_TABLE",
    "iceberg.bronze.order_events_stream",
)
ANOMALY_PREDICTIONS_TABLE = os.getenv(
    "ANOMALY_PREDICTIONS_TABLE",
    "iceberg.gold.order_anomaly_predictions",
)
STREAM_CHECKPOINT_ROOT = os.getenv(
    "STREAM_CHECKPOINT_ROOT",
    str(PROJECT_ROOT / "tmp" / "spark-checkpoints"),
)
