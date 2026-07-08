from __future__ import annotations

import argparse
import csv
import os
import time
import logging
from pathlib import Path

import trino
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

RESULTS_DIR = Path("experiments/results")
CSV_PATH = RESULTS_DIR / "fragmentation_benchmark.csv"

TABLE = "iceberg.bronze.olist_events"
TABLE_CATALOG_SCHEMA, TABLE_NAME = TABLE.rsplit(".", 1)
SMALL_FILE_THRESHOLD_BYTES = int(os.getenv("SMALL_FILE_THRESHOLD_BYTES", 10 * 1024 * 1024))

FIELDNAMES = [
    "run_label",
    "file_count",
    "small_file_count",
    "avg_file_size_kb",
    "min_file_size_kb",
    "max_file_size_kb",
    "snapshot_count",
    "manifest_count",
    "planning_time_ms",
    "cpu_time_ms",
    "elapsed_time_ms",
]


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )


def _file_stats(cur) -> dict:
    cur.execute(
        f'SELECT COUNT(*), '
        f'COUNT(*) FILTER (WHERE file_size_in_bytes < {SMALL_FILE_THRESHOLD_BYTES}), '
        f'AVG(file_size_in_bytes), MIN(file_size_in_bytes), MAX(file_size_in_bytes) '
        f'FROM {TABLE_CATALOG_SCHEMA}."{TABLE_NAME}$files"'
    )
    file_count, small_file_count, avg_bytes, min_bytes, max_bytes = cur.fetchone()
    return {
        "file_count": file_count or 0,
        "small_file_count": small_file_count or 0,
        "avg_file_size_kb": round((avg_bytes or 0) / 1024.0, 2),
        "min_file_size_kb": round((min_bytes or 0) / 1024.0, 2),
        "max_file_size_kb": round((max_bytes or 0) / 1024.0, 2),
    }


def _snapshot_count(cur) -> int:
    cur.execute(f'SELECT COUNT(*) FROM {TABLE_CATALOG_SCHEMA}."{TABLE_NAME}$snapshots"')
    return cur.fetchone()[0]


def _manifest_count(cur) -> int:
    cur.execute(f'SELECT COUNT(*) FROM {TABLE_CATALOG_SCHEMA}."{TABLE_NAME}$manifests"')
    return cur.fetchone()[0]


def _query_cost(cur) -> dict:
    """Parse EXPLAIN ANALYZE output for planning/CPU/elapsed time on a full
    table scan with a filter — the query shape most sensitive to manifest
    and small-file bloat, since Trino must open every manifest and file
    before it can start pruning.
    """
    cur.execute(f"EXPLAIN ANALYZE SELECT COUNT(*) FROM {TABLE} WHERE event_type = 'purchase'")
    rows = [r[0] for r in cur.fetchall()]
    text = "\n".join(rows)

    def _extract_ms(marker: str) -> float:
        for line in rows:
            if marker in line:
                # lines look like: "Planning Time: 123.45ms" or similar variants
                for token in line.replace(",", "").split():
                    if token.endswith("ms"):
                        try:
                            return float(token[:-2])
                        except ValueError:
                            continue
        return -1.0

    return {
        "planning_time_ms": _extract_ms("Planning"),
        "cpu_time_ms": _extract_ms("CPU:"),
        "elapsed_time_ms": _extract_ms("Elapsed:"),
    }, text


def run_benchmark(run_label: str, save_explain: bool = True) -> dict:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    conn = get_connection()
    cur = conn.cursor()

    print("\n" + "=" * 90)
    print(f" FRAGMENTATION BENCHMARK — run: {run_label} ".center(90, "="))
    print("=" * 90)

    row = {"run_label": run_label}

    try:
        row.update(_file_stats(cur))
        row["snapshot_count"] = _snapshot_count(cur)
        row["manifest_count"] = _manifest_count(cur)
    except Exception as e:
        logger.error("Failed to read table metadata for %s: %s", TABLE, e)
        cur.close()
        conn.close()
        raise

    try:
        cost, explain_text = _query_cost(cur)
        row.update(cost)
        if save_explain:
            explain_path = RESULTS_DIR / f"explain_{run_label}.txt"
            explain_path.write_text(explain_text, encoding="utf-8")
    except Exception as e:
        logger.warning("EXPLAIN ANALYZE failed: %s", e)
        row.update({"planning_time_ms": -1, "cpu_time_ms": -1, "elapsed_time_ms": -1})

    cur.close()
    conn.close()

    for key in FIELDNAMES:
        print(f"  {key:<20} {row.get(key)}")
    print("=" * 90 + "\n")

    _append_csv(row)
    return row


def _append_csv(row: dict) -> None:
    file_exists = CSV_PATH.exists()
    with open(CSV_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)
    logger.info("Appended result → %s", CSV_PATH)


def print_comparison() -> None:
    if not CSV_PATH.exists():
        logger.warning("No results yet at %s", CSV_PATH)
        return

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if not rows:
        return

    print("\n" + "=" * 110)
    print(" COMPARISON ACROSS RUNS ".center(110, "="))
    print("=" * 110)
    header = f"  {'run_label':<20}" + "".join(f"{k:>14}" for k in FIELDNAMES[1:])
    print(header)
    print("  " + "-" * 106)
    for r in rows:
        line = f"  {r['run_label']:<20}" + "".join(f"{r.get(k, ''):>14}" for k in FIELDNAMES[1:])
        print(line)
    print("=" * 110 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Measure Iceberg bronze table fragmentation.")
    parser.add_argument(
        "run_label",
        nargs="?",
        default=None,
        help="Label for this run (e.g. baseline, layer1, layer1_layer2). "
             "Omit to just print the comparison table of previously recorded runs.",
    )
    args = parser.parse_args()

    if args.run_label is None:
        print_comparison()
    else:
        run_benchmark(args.run_label)
        print_comparison()
