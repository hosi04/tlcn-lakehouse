from __future__ import annotations

import os
import logging
import trino
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

# Tables fed by streaming / high-frequency micro-batches accumulate small
# files and snapshots much faster than the batch-loaded tables below, so
# they are compacted on a short, threshold-checked schedule instead of the
# fixed weekly sweep.
STREAMING_TABLES = [
    "iceberg.bronze.olist_events",
    "iceberg.silver.events",
]

BATCH_TABLES = [
    "iceberg.gold.fact_order",
    "iceberg.gold.fact_order_item",
    "iceberg.gold.dim_customer",
    "iceberg.gold.dim_product",
    "iceberg.gold.dim_seller",
    "iceberg.gold.agg_daily_funnel",
    "iceberg.gold.agg_product_engagement",
    "iceberg.gold.agg_search_keywords",
]

SMALL_FILE_THRESHOLD_BYTES = int(os.getenv("SMALL_FILE_THRESHOLD_BYTES", 10 * 1024 * 1024))  # 10 MB
SMALL_FILE_COUNT_TRIGGER = int(os.getenv("SMALL_FILE_COUNT_TRIGGER", 20))


def _count_small_files(cur, table: str) -> int:
    cur.execute(
        f'SELECT COUNT(*) FROM "{table}$files" WHERE file_size_in_bytes < {SMALL_FILE_THRESHOLD_BYTES}'
    )
    return cur.fetchone()[0]


def _optimize(cur, table: str) -> None:
    logger.info("  -> Running compaction (OPTIMIZE)...")
    try:
        cur.execute(f"ALTER TABLE {table} EXECUTE OPTIMIZE")
        logger.info("     OK: compaction done.")
    except Exception as e:
        logger.warning("     Skipped OPTIMIZE: %s", e)


def _expire_snapshots(cur, table: str, retention: str) -> None:
    logger.info("  -> Running vacuum (expire snapshots older than %s)...", retention)
    try:
        cur.execute(f"ALTER TABLE {table} EXECUTE EXPIRE_SNAPSHOTS(retention_threshold => '{retention}')")
        logger.info("     OK: vacuum done.")
    except Exception as e:
        logger.warning("     Skipped EXPIRE_SNAPSHOTS: %s", e)


def maintain_streaming_tables(cur) -> None:
    """Threshold-checked compaction for streaming-fed tables.

    Meant to run on a short interval (e.g. every 15-30 min). Skips OPTIMIZE
    when the table isn't fragmented enough yet, so a frequent schedule
    doesn't burn compute rewriting tables that don't need it.
    """
    for table in STREAMING_TABLES:
        logger.info("\n--- %s (streaming) ---", table)
        try:
            small_files = _count_small_files(cur, table)
        except Exception as e:
            logger.warning("  Skipped, table not readable yet: %s", e)
            continue

        logger.info("  Small files (<%s bytes): %s", SMALL_FILE_THRESHOLD_BYTES, small_files)
        if small_files < SMALL_FILE_COUNT_TRIGGER:
            logger.info("  Below trigger threshold (%s) - skipping.", SMALL_FILE_COUNT_TRIGGER)
            continue

        _optimize(cur, table)
        _expire_snapshots(cur, table, retention="1h")


def maintain_batch_tables(cur) -> None:
    """Unconditional weekly compaction for batch-loaded gold/dim/fact tables."""
    for table in BATCH_TABLES:
        logger.info("\n--- %s (batch) ---", table)
        _optimize(cur, table)
        _expire_snapshots(cur, table, retention="7d")


def _connect():
    logger.info("Connecting to Trino at %s:%s", TRINO_HOST, TRINO_PORT)
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )


def run_streaming_maintenance() -> bool:
    logger.info("=" * 80)
    logger.info(" STREAMING TABLE MAINTENANCE (BRONZE/SILVER) ".center(80, "="))
    logger.info("=" * 80)
    conn = _connect()
    cur = conn.cursor()
    maintain_streaming_tables(cur)
    cur.close()
    conn.close()
    logger.info("\nStreaming table maintenance finished.")
    return True


def run_batch_maintenance() -> bool:
    logger.info("=" * 80)
    logger.info(" BATCH TABLE MAINTENANCE (GOLD) ".center(80, "="))
    logger.info("=" * 80)
    conn = _connect()
    cur = conn.cursor()
    maintain_batch_tables(cur)
    cur.close()
    conn.close()
    logger.info("\nBatch table maintenance finished.")
    return True


if __name__ == "__main__":
    import sys

    mode = sys.argv[1] if len(sys.argv) > 1 else "batch"
    if mode == "streaming":
        run_streaming_maintenance()
    else:
        run_batch_maintenance()
