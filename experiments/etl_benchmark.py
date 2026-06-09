from __future__ import annotations

import os
import time
import csv
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8085"))
TRINO_USER = os.getenv("TRINO_USER", "admin")

RESULTS_DIR = Path("experiments/results")

TABLES = [
    ("Bronze", "iceberg.bronze.olist_customers_dataset"),
    ("Bronze", "iceberg.bronze.olist_sellers_dataset"),
    ("Bronze", "iceberg.bronze.olist_products_dataset"),
    ("Bronze", "iceberg.bronze.olist_orders_dataset"),
    ("Bronze", "iceberg.bronze.olist_order_items_dataset"),
    ("Bronze", "iceberg.bronze.olist_order_payments_dataset"),
    ("Bronze", "iceberg.bronze.olist_order_reviews_dataset"),
    ("Silver", "iceberg.silver.customers"),
    ("Silver", "iceberg.silver.sellers"),
    ("Silver", "iceberg.silver.products"),
    ("Silver", "iceberg.silver.orders"),
    ("Silver", "iceberg.silver.order_items"),
    ("Silver", "iceberg.silver.payments"),
    ("Silver", "iceberg.silver.order_reviews"),
    ("Gold",   "iceberg.gold.dim_customer"),
    ("Gold",   "iceberg.gold.dim_seller"),
    ("Gold",   "iceberg.gold.dim_product"),
    ("Gold",   "iceberg.gold.dim_date"),
    ("Gold",   "iceberg.gold.fact_order"),
    ("Gold",   "iceberg.gold.fact_order_item"),
]

DATA_QUALITY_CHECKS = [
    {
        "name": "dim_customer_pk_not_null",
        "sql": "SELECT COUNT(*) FROM iceberg.gold.dim_customer WHERE customer_key IS NULL",
        "expected": 0,
    },
    {
        "name": "dim_product_pk_not_null",
        "sql": "SELECT COUNT(*) FROM iceberg.gold.dim_product WHERE product_key IS NULL",
        "expected": 0,
    },
    {
        "name": "dim_seller_pk_not_null",
        "sql": "SELECT COUNT(*) FROM iceberg.gold.dim_seller WHERE seller_key IS NULL",
        "expected": 0,
    },
    {
        "name": "fact_order_customer_fk",
        "sql": """
            SELECT COUNT(*)
            FROM iceberg.gold.fact_order fo
            LEFT JOIN iceberg.gold.dim_customer dc
                ON fo.customer_key = dc.customer_key
            WHERE dc.customer_key IS NULL
        """,
        "expected": 0,
    },
    {
        "name": "fact_order_item_product_fk",
        "sql": """
            SELECT COUNT(*)
            FROM iceberg.gold.fact_order_item foi
            LEFT JOIN iceberg.gold.dim_product dp
                ON foi.product_key = dp.product_key
            WHERE dp.product_key IS NULL
        """,
        "expected": 0,
    },
    {
        "name": "fact_order_non_negative_values",
        "sql": """
            SELECT COUNT(*)
            FROM iceberg.gold.fact_order
            WHERE total_payment_value < 0
               OR total_product_value < 0
               OR total_freight_value < 0
        """,
        "expected": 0,
    },
    {
        "name": "fact_order_vs_item_product_total",
        "sql": """
            SELECT CASE
                WHEN ABS(
                    (SELECT COALESCE(SUM(total_product_value), 0) FROM iceberg.gold.fact_order)
                    - (SELECT COALESCE(SUM(item_price), 0) FROM iceberg.gold.fact_order_item)
                ) <= 0.01
                THEN 0 ELSE 1
            END
        """,
        "expected": 0,
    },
]


def get_connection():
    import trino

    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        http_scheme="http",
    )


def query_table_stats(cur, full_table: str) -> dict:
    t0 = time.perf_counter()
    cur.execute(f"SELECT COUNT(*) FROM {full_table}")
    row_count = cur.fetchone()[0]
    elapsed = time.perf_counter() - t0

    parts = full_table.split(".")
    catalog, schema, table = parts[0], parts[1], parts[2]
    cur.execute(
        f"SELECT COUNT(*) FROM {catalog}.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name = '{table}'"
    )
    col_count = cur.fetchone()[0]

    return {
        "row_count": row_count,
        "col_count": col_count,
        "elapsed_s": elapsed,
    }


def run_data_quality_checks(cur) -> tuple[bool, list[dict]]:
    print("\n" + "=" * 90)
    print(" DATA QUALITY CHECKS ".center(90, "="))
    print("=" * 90)
    print(f"  {'Check':<42} {'Actual':>12} {'Expected':>12} {'Status':>10}")
    print("  " + "-" * 80)

    rows = []
    all_passed = True
    for check in DATA_QUALITY_CHECKS:
        try:
            cur.execute(check["sql"])
            actual = cur.fetchone()[0]
            expected = check["expected"]
            passed = actual == expected
            all_passed = all_passed and passed
            status = "PASS" if passed else "FAIL"
            print(f"  {check['name']:<42} {actual:>12} {expected:>12} {status:>10}")
            rows.append({
                "check": check["name"],
                "actual": actual,
                "expected": expected,
                "status": status,
                "error": "",
            })
        except Exception as e:
            all_passed = False
            print(f"  {check['name']:<42} {'ERROR':>12} {check['expected']:>12} {'FAIL':>10}")
            rows.append({
                "check": check["name"],
                "actual": "",
                "expected": check["expected"],
                "status": "FAIL",
                "error": str(e),
            })

    print("=" * 90 + "\n")
    return all_passed, rows


def run_etl_benchmark():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 90)
    print(" ETL PIPELINE BENCHMARK — Lakehouse Medallion Architecture ".center(90, "="))
    print("=" * 90)
    print(f"  Kết nối Trino tại {TRINO_HOST}:{TRINO_PORT}")
    print(
        f"\n  {'Layer':<8} {'Table':<40} {'Rows':>10} {'Cols':>5} {'Time (s)':>10}"
    )
    print("  " + "-" * 76)

    results = []
    conn = get_connection()
    cur = conn.cursor()

    for layer, full_table in TABLES:
        table_name = full_table.split(".")[-1]
        try:
            stats = query_table_stats(cur, full_table)
            print(
                f"  {layer:<8} {table_name:<40} {stats['row_count']:>10,} "
                f"{stats['col_count']:>5} {stats['elapsed_s']:>10.3f}"
            )
            results.append({
                "layer": layer,
                "table": table_name,
                "full_table": full_table,
                "rows": stats["row_count"],
                "cols": stats["col_count"],
                "query_time_s": round(stats["elapsed_s"], 3),
                "status": "OK",
            })
        except Exception as e:
            print(f"  {layer:<8} {table_name:<40} {'ERROR':>10}  {'':>5} {'':>10}  ← {e}")
            results.append({
                "layer": layer,
                "table": table_name,
                "full_table": full_table,
                "rows": -1,
                "cols": -1,
                "query_time_s": -1,
                "status": f"ERROR: {e}",
            })

    quality_ok, quality_rows = run_data_quality_checks(cur)

    cur.close()
    conn.close()
    print("  " + "-" * 76)
    print("=" * 90 + "\n")

    csv_path = RESULTS_DIR / "etl_benchmark.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=results[0].keys())
        writer.writeheader()
        writer.writerows(results)

    logger.info("ETL benchmark saved → %s", csv_path)

    quality_csv_path = RESULTS_DIR / "etl_quality_checks.csv"
    with open(quality_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["check", "actual", "expected", "status", "error"],
        )
        writer.writeheader()
        writer.writerows(quality_rows)

    tables_ok = all(row["status"] == "OK" for row in results)
    logger.info("ETL quality checks saved → %s", quality_csv_path)
    return tables_ok and quality_ok


if __name__ == "__main__":
    raise SystemExit(0 if run_etl_benchmark() else 1)
