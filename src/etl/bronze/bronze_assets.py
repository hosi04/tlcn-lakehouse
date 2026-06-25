import logging
import os
from pathlib import Path

import yaml
from dotenv import load_dotenv
from minio import Minio

from src.etl.utils import get_spark_session

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _PROJECT_ROOT / "config" / "bronze_sources.yaml"
_BRONZE_NAMESPACE = "iceberg.bronze"



def _load_config() -> dict:
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Bronze config not found: {_CONFIG_PATH}\n"
            "Make sure config/bronze_sources.yaml exists."
        )
    with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def create_bucket_if_not_exists(bucket_name: str) -> None:
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000").replace("http://", ""),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
        secure=False,
    )
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info("Bucket '%s' created.", bucket_name)
    else:
        logger.info("Bucket '%s' already exists.", bucket_name)



def _ingest_source(spark, source: dict, data_root: Path) -> dict:
    name = source["name"]
    file_path = data_root / source["path"]
    fmt = source.get("format", "csv").lower()
    options = source.get("options", {})
    target_table = source.get("target", name)
    full_target = f"{_BRONZE_NAMESPACE}.{target_table}"

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found for '{name}': {file_path}\n"
            f"Check DATA_ROOT ({data_root}) and source.path in bronze_sources.yaml."
        )

    logger.info("Reading [%s] ← %s", fmt, file_path)

    reader = spark.read.format(fmt)
    for key, val in options.items():
        reader = reader.option(key, val)
    df = reader.load(str(file_path))

    row_count = df.count()
    col_count = len(df.columns)

    logger.info(
        "Writing → %s | rows=%d | cols=%d",
        full_target, row_count, col_count,
    )

    df.write.format("iceberg").mode("overwrite").saveAsTable(full_target)

    logger.info("Done: %s", full_target)
    return {
        "name": name,
        "target": full_target,
        "row_count": row_count,
        "column_count": col_count,
    }


if __name__ == "__main__":
    load_dotenv()

    data_root = Path(os.getenv("DATA_ROOT", "./data")).resolve()
    logger.info("DATA_ROOT = %s", data_root)

    config = _load_config()
    sources = config.get("sources", [])
    if not sources:
        raise ValueError("No sources defined in bronze_sources.yaml.")

    logger.info("Bronze pipeline: %d source(s) to ingest.", len(sources))

    create_bucket_if_not_exists("lakehouse")

    spark = get_spark_session("Bronze Ingest")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {_BRONZE_NAMESPACE}")

    results = []
    failed = []

    try:
        for source in sources:
            try:
                meta = _ingest_source(spark, source, data_root)
                results.append(meta)
            except Exception as exc:
                logger.error("Failed '%s': %s", source.get("name", "?"), exc)
                failed.append({"name": source.get("name", "?"), "error": str(exc)})
    finally:
        spark.stop()

    logger.info(
        "Bronze pipeline finished. success=%d  failed=%d",
        len(results), len(failed),
    )
    if failed:
        logger.warning("Failed sources: %s", [f["name"] for f in failed])
        import sys
        sys.exit(1)
