import argparse
import logging
import shutil
from pathlib import Path

import pandas as pd

from src.streaming.config import BATCH_DATA_ROOT, DATA_ROOT, STREAM_DATA_ROOT


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

ORDER_TABLES = {
    "olist_orders_dataset.csv": "order_id",
    "olist_order_items_dataset.csv": "order_id",
    "olist_order_payments_dataset.csv": "order_id",
}

DIMENSION_TABLES = [
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_products_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "product_category_name_translation.csv",
    "olist_geolocation_dataset.csv",
]


def _write_filtered_csv(source: Path, target: Path, key_col: str, keys: set[str]) -> int:
    df = pd.read_csv(source)
    filtered = df[df[key_col].isin(keys)]
    filtered.to_csv(target, index=False)
    return len(filtered)


def split_batch_stream_data(
    source_root: Path,
    batch_root: Path,
    stream_root: Path,
    train_ratio: float,
) -> None:
    source_root = source_root.resolve()
    batch_root = batch_root.resolve()
    stream_root = stream_root.resolve()

    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1")

    orders_path = source_root / "olist_orders_dataset.csv"
    if not orders_path.exists():
        raise FileNotFoundError(f"Missing source orders file: {orders_path}")

    batch_root.mkdir(parents=True, exist_ok=True)
    stream_root.mkdir(parents=True, exist_ok=True)

    orders = pd.read_csv(orders_path, parse_dates=["order_purchase_timestamp"])
    orders = orders.sort_values("order_purchase_timestamp").reset_index(drop=True)

    split_idx = int(len(orders) * train_ratio)
    batch_order_ids = set(orders.iloc[:split_idx]["order_id"].astype(str))
    stream_order_ids = set(orders.iloc[split_idx:]["order_id"].astype(str))

    logger.info(
        "Split orders chronologically: batch=%s stream=%s ratio=%.2f",
        len(batch_order_ids),
        len(stream_order_ids),
        train_ratio,
    )

    for filename, key_col in ORDER_TABLES.items():
        source = source_root / filename
        if not source.exists():
            logger.warning("Skipping missing source file: %s", source)
            continue

        batch_count = _write_filtered_csv(source, batch_root / filename, key_col, batch_order_ids)
        stream_count = _write_filtered_csv(source, stream_root / filename, key_col, stream_order_ids)
        logger.info("%s -> batch=%s stream=%s", filename, batch_count, stream_count)

    for filename in DIMENSION_TABLES:
        source = source_root / filename
        if not source.exists():
            logger.warning("Skipping missing dimension file: %s", source)
            continue
        shutil.copy2(source, batch_root / filename)
        logger.info("%s -> copied to batch", filename)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Split Olist CSV data into chronological batch and stream subsets."
    )
    parser.add_argument("--source-root", type=Path, default=DATA_ROOT)
    parser.add_argument("--batch-root", type=Path, default=BATCH_DATA_ROOT)
    parser.add_argument("--stream-root", type=Path, default=STREAM_DATA_ROOT)
    parser.add_argument("--train-ratio", type=float, default=0.8)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    split_batch_stream_data(
        source_root=args.source_root,
        batch_root=args.batch_root,
        stream_root=args.stream_root,
        train_ratio=args.train_ratio,
    )
