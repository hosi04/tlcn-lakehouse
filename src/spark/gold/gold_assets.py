from config.spark_config import SparkConnect
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
import os
from dotenv import load_dotenv


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


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

def read_from_iceberg(spark, table_name: str, namespace="iceberg.silver") -> DataFrame:
    """Read data from Iceberg silver layer"""
    full_table_name = f"{namespace}.{table_name}"
    logger.info(f"Reading from {full_table_name}")
    return spark.read.format("iceberg").load(full_table_name)

def write_to_iceberg(spark, df: DataFrame, table_name: str, namespace="iceberg.gold"):
    """Write data to Iceberg gold layer"""
    full_table_name = f"{namespace}.{table_name}"
    col_count = len(df.columns)
    
    logger.info(f"Writing to {full_table_name} ({col_count} columns)")
    df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)

    try:
        row_count = spark.table(full_table_name).count()
    except Exception as e:
        logger.warning(f"Unable to fetch row count: {e}")
        row_count = -1
    
    logger.info(f"Successfully wrote data to {full_table_name}")
    return {
        "table": table_name,
        "row_count": row_count,
        "column_count": col_count,
        "columns": df.columns,
    }


def dim_order(spark):
    logger.info("Building dim_order ...")
    df_orders = read_from_iceberg(spark, "orders")
    df_payments = read_from_iceberg(spark, "payments")
    df = df_orders.join(df_payments, "order_id", how="inner")
    df = df.select(
        "order_id",
        "order_status",
        "payment_type"
    )
    metadata = write_to_iceberg(spark, df, "dim_order")
    return df, metadata

def dim_customer(spark):
    logger.info("Building dim_customer ...")
    df_customers = read_from_iceberg(spark, "customers")
    df_geolocation = read_from_iceberg(spark, "geolocation")
    df = df_customers.join(df_geolocation, 
            df_customers["customer_zip_code_prefix"] == df_geolocation["geolocation_zip_code_prefix"],
            how="left"
            )
    df = df.withColumnRenamed(
            "geolocation_lat", "customer_lat"
        ).withColumnRenamed(
            "geolocation_lng", "customer_lng"
        )
    df = df.drop(
        "geolocation_city",
        "geolocation_state",
        "geolocation_zip_code_prefix",
        "customer_zip_code_prefix"
    )
    df = df.dropDuplicates(subset=["customer_id"])
    metadata = write_to_iceberg(spark, df, "dim_customer")
    return df, metadata

def dim_seller(spark):
    logger.info("Building dim_seller")
    df_sellers = read_from_iceberg(spark, "sellers")
    metadata = write_to_iceberg(spark, df_sellers, "dim_seller")
    return df_sellers, metadata

def dim_product(spark):
    logger.info("Building dim_product ...")
    
    df_products = read_from_iceberg(spark, "products").alias("p")
    df_product_category = read_from_iceberg(spark, "product_category").alias("pc")
    

    df = df_products.join(
        df_product_category,
        df_products["product_category_name"] == df_product_category["product_category_name"],
        "inner"
    )
    
    df = df.select(
        df_products["product_id"],
        df_product_category["product_category_name"].alias("product_category_name"),
        df_product_category["product_category_name_english"],
        df_products["product_name_length"],
        df_products["product_description_length"],
        df_products["product_photos_qty"],
        df_products["product_weight_g"],
        df_products["product_length_cm"],
        df_products["product_height_cm"],
        df_products["product_width_cm"]
    )
    
    metadata = write_to_iceberg(spark, df, "dim_product")
    return df, metadata


def dim_review(spark):
    logger.info("Building dim_review ...")
    df_review = read_from_iceberg(spark, "order_reviews")
    df = df_review.select(
        "review_id",
        "review_score"
    )
    metadata = write_to_iceberg(spark, df, "dim_review")
    return df, metadata

def dim_date(spark):
    logger.info("Building dim_date ...")
    df_date = read_from_iceberg(spark, "date")
    range_df = df_date.select(
        min("order_purchase_timestamp").alias("min_date"),
        max("order_purchase_timestamp").alias("max_date")
    )
    df = (
        range_df
        .select(
            explode(
                sequence(
                    col("min_date").cast("date"),
                    col("max_date").cast("date"),
                    expr("INTERVAL 1 DAY")
                )
            ).alias("date")
        )
        .withColumn("date_key", year(col("date")) * 10000 + month(col("date")) * 100 + dayofmonth(col("date")))
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
        .withColumn("month", month("date"))
        .withColumn("day", dayofmonth("date"))
        .withColumn("day_of_week", (weekday("date") + 1).cast("int"))
        .withColumn("week_of_year", weekofyear("date"))
        .withColumn("month_name", date_format("date", "MMMM"))
        .withColumn("day_name", date_format("date", "EEEE"))
        .withColumn("is_weekend", col("day_of_week").isin([6, 7]))
    )
    metadata = write_to_iceberg(spark, df, "dim_date")
    return df, metadata

def fact_sales(spark):
    logger.info("Building fact_sales ....")

    df_order = read_from_iceberg(spark, "orders", namespace="iceberg.silver")
    df_order_items = read_from_iceberg(spark, "order_items", namespace="iceberg.silver")
    df_payments = read_from_iceberg(spark, "payments", namespace="iceberg.silver")
    df_reviews = read_from_iceberg(spark, "order_reviews", namespace="iceberg.silver")
    dim_order = read_from_iceberg(spark, "dim_order", namespace="iceberg.gold")
    dim_customer = read_from_iceberg(spark, "dim_customer", namespace="iceberg.gold")
    dim_product = read_from_iceberg(spark, "dim_product", namespace="iceberg.gold")
    dim_seller = read_from_iceberg(spark, "dim_seller", namespace="iceberg.gold")
    dim_date = read_from_iceberg(spark, "dim_date", namespace="iceberg.gold")


    df_base = df_order.join(df_order_items, "order_id", "inner")

    df = (
        df_base
        .join(dim_order, ["order_id"], "inner")
        .join(dim_product, "product_id", "inner")
        .join(dim_customer, "customer_id", "inner")
        .join(dim_seller, "seller_id", "inner")
        .join(df_payments, "order_id", "inner")
        .join(df_reviews, "order_id", "inner")  
        .join(
            dim_date,
            df_base["order_purchase_timestamp"].cast("date") == dim_date["date"],
            "inner"
        )
    )

    df = df.select(
        "order_id",
        "order_item_id",
        "customer_id",
        "product_id",
        "seller_id",
        "review_id",
        "date_key",
        "price",
        "freight_value",
        "payment_value",
        "payment_installments",
        "payment_sequential",
    )

    metadata = write_to_iceberg(spark, df, "fact_sales")
    return df, metadata

if __name__ == "__main__":
    load_dotenv()
    spark = init_spark()

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    
    logger.info("Starting Gold layer processing...")
    
    try:
        dim_order(spark)
        dim_customer(spark)
        dim_seller(spark)
        dim_product(spark)
        dim_review(spark)
        dim_date(spark)
        fact_sales(spark)
        logger.info("Gold layer processing completed successfully!")
    except Exception as e:
        logger.error(f"Error during Gold layer processing: {e}")
        raise
    finally:
        spark.stop()
