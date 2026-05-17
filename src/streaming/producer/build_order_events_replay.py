import argparse
import logging
import uuid
from pathlib import Path

import pandas as pd

from src.streaming.config import STREAM_DATA_ROOT


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _event_id(order_id: str, seller_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"olist:{order_id}:{seller_id}"))


def build_order_events_replay(stream_root: Path, output_path: Path) -> pd.DataFrame:
    stream_root = stream_root.resolve()
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(stream_root / "olist_orders_dataset.csv")
    items = pd.read_csv(stream_root / "olist_order_items_dataset.csv")
    payments = pd.read_csv(stream_root / "olist_order_payments_dataset.csv")

    item_events = (
        items.groupby(["order_id", "seller_id"], as_index=False)
        .agg(
            product_id=("product_id", "first"),
            number_of_items=("order_item_id", "count"),
            total_product_value=("price", "sum"),
            total_freight_value=("freight_value", "sum"),
        )
    )

    payment_events = (
        payments.groupby("order_id", as_index=False)
        .agg(
            payment_value=("payment_value", "sum"),
            payment_installments=("payment_installments", "sum"),
            payment_count=("payment_sequential", "count"),
            primary_payment_type=("payment_type", "first"),
        )
    )

    events = (
        orders.merge(item_events, on="order_id", how="inner")
        .merge(payment_events, on="order_id", how="left")
        .sort_values("order_purchase_timestamp")
        .reset_index(drop=True)
    )

    events["event_id"] = [
        _event_id(order_id, seller_id)
        for order_id, seller_id in zip(events["order_id"], events["seller_id"])
    ]
    events["event_type"] = "order_seller_snapshot"
    events["event_time"] = events["order_purchase_timestamp"]
    events["created_at"] = pd.Timestamp.utcnow().isoformat()

    ordered_columns = [
        "event_id",
        "event_type",
        "event_time",
        "order_id",
        "customer_id",
        "seller_id",
        "product_id",
        "order_status",
        "order_purchase_timestamp",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
        "number_of_items",
        "total_product_value",
        "total_freight_value",
        "payment_value",
        "payment_installments",
        "payment_count",
        "primary_payment_type",
        "created_at",
    ]
    events = events[ordered_columns]
    events.to_csv(output_path, index=False)

    logger.info("Built replay dataset: %s rows -> %s", len(events), output_path)
    return events


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build denormalized order events for Kafka replay."
    )
    parser.add_argument("--stream-root", type=Path, default=STREAM_DATA_ROOT)
    parser.add_argument(
        "--output-path",
        type=Path,
        default=STREAM_DATA_ROOT / "order_events_replay.csv",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_order_events_replay(args.stream_root, args.output_path)
