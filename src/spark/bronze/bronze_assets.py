# file: bronze_assets.py
from config.spark_config import SparkConnect
from dotenv import load_dotenv
import os
from minio import Minio
import pandas as pd
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_bucket_if_not_exists(bucket_name: str):
    minio_client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False
    )
    if not minio_client.bucket_exists(bucket_name=bucket_name):
        minio_client.make_bucket(bucket_name=bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def init_spark():
    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "software.amazon.awssdk:s3:2.20.125",
        "org.apache.hadoop:hadoop-aws:3.3.1"
    ]
    spark_conf = {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.catalog.iceberg": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg.type": "hive",
        "spark.sql.catalog.iceberg.uri": "thrift://localhost:9083",
        "spark.sql.catalog.iceberg.warehouse": "s3a://lakehouse",

        # S3A (MinIO)
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.s3a.impl": "org.apache.hadoop.fs.s3a.S3A",
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY"),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # Map scheme `s3://` -> dùng driver S3A (để đọc các location `s3://` do HMS/Trino ghi)
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.s3.impl": "org.apache.hadoop.fs.s3a.S3A",
        "spark.hadoop.fs.s3.endpoint": os.getenv("MINIO_ENDPOINT"),
        "spark.hadoop.fs.s3.path.style.access": "true",
        "spark.hadoop.fs.s3.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3.access.key": os.getenv("MINIO_ACCESS_KEY"),
        "spark.hadoop.fs.s3.secret.key": os.getenv("MINIO_SECRET_KEY"),
    }
    spark = SparkConnect(
        app_name="Bronze Ingest",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark
    return spark

def write_csv_to_iceberg(spark, file_path: str, namespace="iceberg.bronze"):
    import os
    table_name = os.path.splitext(os.path.basename(file_path))[0]
    full_table_name = f"{namespace}.{table_name}"
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
    df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
    print(f"Data from {file_path} written to {full_table_name} ({df.count()} rows)")

def bronze_customers(spark): 
    write_csv_to_iceberg(spark, "./data/olist_customers_dataset.csv")

def bronze_sellers(spark): 
    write_csv_to_iceberg(spark, "./data/olist_sellers_dataset.csv")

def bronze_products(spark): 
    write_csv_to_iceberg(spark, "./data/olist_products_dataset.csv")

def bronze_orders(spark): 
    write_csv_to_iceberg(spark, "./data/olist_orders_dataset.csv")

def bronze_order_items(spark): 
    write_csv_to_iceberg(spark, "./data/olist_order_items_dataset.csv")

def bronze_payments(spark): 
    write_csv_to_iceberg(spark, "./data/olist_order_payments_dataset.csv")

def bronze_order_reviews(spark): 
    write_csv_to_iceberg(spark, "./data/olist_order_reviews_dataset.csv")

def bronze_product_category(spark): 
    write_csv_to_iceberg(spark, "./data/product_category_name_translation.csv")

def bronze_geolocation(spark): 
    write_csv_to_iceberg(spark, "./data/olist_geolocation_dataset.csv")



if __name__ == "__main__":

    load_dotenv()
    create_bucket_if_not_exists("lakehouse")
    spark = init_spark()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    logger.info("Starting Bronze layer processing...")
    
    try:
        bronze_customers(spark)
        bronze_sellers(spark)
        bronze_products(spark)
        bronze_orders(spark)
        bronze_order_items(spark)
        bronze_payments(spark)
        bronze_order_reviews(spark)
        bronze_product_category(spark)
        bronze_geolocation(spark)
        
        logger.info("Bronze layer processing completed successfully!")
    except Exception as e:
        logger.error(f"Error during bronze layer processing: {e}")
        raise
    finally:
        spark.stop()