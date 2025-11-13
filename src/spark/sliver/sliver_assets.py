from config.spark_config import SparkConnect
from dotenv import load_dotenv
import os
from pyspark.sql.functions import col, round as spark_round
from pyspark.sql import DataFrame
import logging
from minio import Minio


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
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1",
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

        # Memory & Iceberg
        "spark.executor.memoryOverhead": "2g",
        "spark.sql.iceberg.direct-write.enabled": "false",
        "spark.sql.iceberg.vectorization.enabled": "false",
    }
    spark = SparkConnect(
        app_name="Bronze Ingest",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="8g",
        num_executors=1,
        jar_packages=jar_packages,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark
    return spark


def read_from_iceberg(spark, table_name: str, namespace="iceberg.bronze") -> DataFrame:
    """Read data from Iceberg bronze layer"""
    full_table_name = f"{namespace}.{table_name}"
    logger.info(f"Reading from {full_table_name}")
    return spark.read.format("iceberg").load(full_table_name)


def write_to_iceberg(spark, df: DataFrame, table_name: str, namespace="iceberg.silver"):
    """Write data to Iceberg silver layer"""
    full_table_name = f"{namespace}.{table_name}"
    col_count = len(df.columns)
    
    logger.info(f"Ghi vào {full_table_name} ({col_count} cột)")
    df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)

    try:
        row_count = spark.table(full_table_name).count()
    except Exception as e:
        logger.warning(f"Không thể lấy row_count: {e}")
        row_count = -1
    
    logger.info(f"Ghi thành công vào {full_table_name}")
    return {
        "table": table_name,
        "row_count": row_count,
        "column_count": col_count,
        "columns": df.columns,
    }


def silver_cleaned_customer(spark):
    """Clean and process customer data"""
    df = read_from_iceberg(spark, "olist_customers_dataset")
    df = df.dropDuplicates()
    df = df.na.drop()
    metadata = write_to_iceberg(spark, df, "customers")
    return df, metadata


def silver_cleaned_seller(spark):
    """Clean and process seller data"""
    df = read_from_iceberg(spark, "olist_sellers_dataset")
    df = df.na.drop()
    df = df.dropDuplicates(subset=["seller_id"])
    metadata = write_to_iceberg(spark, df, "sellers")
    return df, metadata


def silver_cleaned_product(spark):
    """Clean and process product data"""
    df = read_from_iceberg(spark, "olist_products_dataset")
    df = df.na.drop()
    df = df.dropDuplicates()
    
    # Convert columns to integer
    columns_to_convert = [
        "product_description_length",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
    ]
    for column in columns_to_convert:
        if column in df.columns:
            df = df.withColumn(column, col(column).cast("integer"))
    
    metadata = write_to_iceberg(spark, df, "products")
    return df, metadata


def silver_cleaned_order_item(spark):
    """Clean and process order items data"""
    df = read_from_iceberg(spark, "olist_order_items_dataset")
    df = df.withColumn("price", spark_round(col("price"), 2).cast("double"))
    df = df.withColumn("freight_value", spark_round(col("freight_value"), 2).cast("double"))
    df = df.na.drop()
    df = df.dropDuplicates()
    metadata = write_to_iceberg(spark, df, "order_items")
    return df, metadata


def silver_cleaned_payment(spark):
    """Clean and process payment data"""
    df = read_from_iceberg(spark, "olist_order_payments_dataset")
    df = df.withColumn("payment_value", spark_round(col("payment_value"), 2).cast("double"))
    df = df.withColumn("payment_installments", col("payment_installments").cast("integer"))
    df = df.na.drop()
    df = df.dropDuplicates()
    metadata = write_to_iceberg(spark, df, "payments")
    return df, metadata


def silver_cleaned_order_review(spark):
    """Clean and process order review data"""
    df = read_from_iceberg(spark, "olist_order_reviews_dataset")
    df = df.drop("review_comment_title")
    df = df.na.drop()
    df = df.dropDuplicates()
    metadata = write_to_iceberg(spark, df, "order_reviews")
    return df, metadata


def silver_cleaned_product_category(spark):
    """Clean and process product category data"""
    df = read_from_iceberg(spark, "product_category_name_translation")
    df = df.dropDuplicates()
    df = df.na.drop()
    metadata = write_to_iceberg(spark, df, "product_category")
    return df, metadata


def silver_cleaned_order(spark):
    """Clean and process order data"""
    df = read_from_iceberg(spark, "olist_orders_dataset")
    df = df.na.drop()
    df = df.dropDuplicates(["order_id"])
    metadata = write_to_iceberg(spark, df, "orders")
    return df, metadata


def silver_date(spark):
    """Create date dimension from orders"""
    df = read_from_iceberg(spark, "olist_orders_dataset")
    df = df.select("order_purchase_timestamp")
    df = df.na.drop()
    df = df.dropDuplicates()
    metadata = write_to_iceberg(spark, df, "date")
    return df, metadata


def silver_cleaned_geolocation(spark):
    """Clean and process geolocation data with Brazil boundaries filter"""
    df = read_from_iceberg(spark, "olist_geolocation_dataset")
    df = df.dropDuplicates()
    df = df.na.drop()
    
    # Filter coordinates for Brazil boundaries
    df = df.filter(
        (col("geolocation_lat") <= 5.27438888)
        & (col("geolocation_lng") >= -73.98283055)
        & (col("geolocation_lat") >= -33.75116944)
        & (col("geolocation_lng") <= -34.79314722)
    )
    
    metadata = write_to_iceberg(spark, df, "geolocation")
    return df, metadata


if __name__ == "__main__":
    load_dotenv()
    spark = init_spark()
    create_bucket_if_not_exists("lakehouse")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")
    
    logger.info("Starting Silver layer processing...")
    
    try:
        silver_cleaned_customer(spark)
        silver_cleaned_seller(spark)
        silver_cleaned_product(spark)
        silver_cleaned_order_item(spark)
        silver_cleaned_payment(spark)
        silver_cleaned_order_review(spark)
        silver_cleaned_product_category(spark)
        silver_cleaned_order(spark)
        silver_date(spark)
        silver_cleaned_geolocation(spark)
        
        logger.info("Silver layer processing completed successfully!")
    except Exception as e:
        logger.error(f"Error during silver layer processing: {e}")
        raise
    finally:
        spark.stop()