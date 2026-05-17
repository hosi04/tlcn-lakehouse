# file: bronze_assets.py
import os
import logging
from pathlib import Path
from dotenv import load_dotenv
from minio import Minio
from src.spark.utils import get_spark_session

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_bucket_if_not_exists(bucket_name: str):
    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT").replace("http://", ""),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        minio_client.make_bucket(bucket_name=bucket_name)
        logger.info(f"Bucket '{bucket_name}' created successfully.")
    else:
        logger.info(f"Bucket '{bucket_name}' already exists.")

def write_csv_to_iceberg(spark, file_path: str, table_name: str, namespace="iceberg.bronze"):
    full_table_name = f"{namespace}.{table_name}"
    try:
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
        logger.info(f"Data from {file_path} written to {full_table_name} ({df.count()} rows)")
    except Exception as e:
        logger.error(f"Failed to write {file_path} to {full_table_name}: {e}")
        raise

DATA_ROOT = Path(os.getenv("DATA_ROOT", "./data"))

DATASETS = {
    "olist_customers_dataset": DATA_ROOT / "olist_customers_dataset.csv",
    "olist_sellers_dataset": DATA_ROOT / "olist_sellers_dataset.csv",
    "olist_products_dataset": DATA_ROOT / "olist_products_dataset.csv",
    "olist_orders_dataset": DATA_ROOT / "olist_orders_dataset.csv",
    "olist_order_items_dataset": DATA_ROOT / "olist_order_items_dataset.csv",
    "olist_order_payments_dataset": DATA_ROOT / "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset": DATA_ROOT / "olist_order_reviews_dataset.csv",
    "product_category_name_translation": DATA_ROOT / "product_category_name_translation.csv",
    "olist_geolocation_dataset": DATA_ROOT / "olist_geolocation_dataset.csv",
}

if __name__ == "__main__":
    load_dotenv()
    create_bucket_if_not_exists("lakehouse")
    
    spark = get_spark_session("Bronze Ingest")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    logger.info("Starting Bronze layer processing...")
    
    try:
        logger.info(f"Reading batch CSV files from: {DATA_ROOT}")
        for table_name, file_path in DATASETS.items():
            write_csv_to_iceberg(spark, str(file_path), table_name)
        
        logger.info("Bronze layer processing completed successfully!")
    except Exception as e:
        logger.error(f"Error during bronze layer processing: {e}")
        raise
    finally:
        spark.stop()
