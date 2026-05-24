import logging

from dotenv import load_dotenv

from src.spark.silver.rule_engine import load_yaml_config, apply_rules, summarise_rules
from src.spark.utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

_BRONZE_NAMESPACE = "iceberg.bronze"
_SILVER_NAMESPACE = "iceberg.silver"


def _write_to_silver(spark, df, table_name: str) -> dict:
    full_table = f"{_SILVER_NAMESPACE}.{table_name}"
    col_count = len(df.columns)

    df.write.format("iceberg").mode("overwrite").saveAsTable(full_table)

    try:
        row_count = spark.table(full_table).count()
    except Exception as exc:
        logger.warning("Could not count rows for %s: %s", full_table, exc)
        row_count = -1

    return {
        "table": full_table,
        "row_count": row_count,
        "column_count": col_count,
    }


def _process_table(spark, rule: dict) -> dict:
    source = rule["source"]
    target = rule["target"]
    full_source = f"{_BRONZE_NAMESPACE}.{source}"
    applied_rules = summarise_rules(rule)

    logger.info(
        "── Table: %-25s  source: %-40s  target: iceberg.silver.%s",
        target, full_source, target,
    )
    logger.info("   Applied rules: %s", applied_rules)

    df = spark.read.format("iceberg").load(full_source)
    
    df = apply_rules(df, rule)

    meta = _write_to_silver(spark, df, target)
    meta.update({"source": full_source, "applied_rules": applied_rules})

    logger.info(
        "   ✔ Done | rows=%-8s cols=%s",
        meta["row_count"], meta["column_count"],
    )
    return meta


if __name__ == "__main__":
    load_dotenv()

    config = load_yaml_config()
    tables = config.get("tables", [])
    if not tables:
        raise ValueError("No tables defined in silver_rules.yaml.")

    logger.info("=" * 65)
    logger.info("Silver pipeline: %d table(s) to process.", len(tables))
    logger.info("=" * 65)

    spark = get_spark_session("Silver Ingest")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_SILVER_NAMESPACE}")

    results = []
    failed = []

    try:
        for rule in tables:
            try:
                meta = _process_table(spark, rule)
                results.append(meta)
            except Exception as exc:
                tgt = rule.get("target", "?")
                logger.error("Failed '%s': %s", tgt, exc)
                failed.append({"target": tgt, "error": str(exc)})
    finally:
        spark.stop()

    logger.info("=" * 65)
    logger.info(
        "Silver pipeline finished. success=%d  failed=%d",
        len(results), len(failed),
    )
    if failed:
        logger.warning("Failed tables: %s", [f["target"] for f in failed])