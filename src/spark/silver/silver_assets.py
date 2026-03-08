from pyspark.sql.functions import col, round as spark_round, sum as spark_sum, first, max as spark_max, count, avg
from pyspark.sql import DataFrame
import logging
from dotenv import load_dotenv
from src.spark.utils import get_spark_session

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_from_iceberg(spark, table_name: str, namespace="iceberg.bronze") -> DataFrame:
    """Read data from Iceberg bronze layer"""
    full_table_name = f"{namespace}.{table_name}"
    logger.info(f"Reading from {full_table_name}")
    return spark.read.format("iceberg").load(full_table_name)

def write_to_iceberg(spark, df: DataFrame, table_name: str, namespace="iceberg.silver"):
    """Write a Spark DataFrame to an Iceberg table in the Silver layer."""
    full_table_name = f"{namespace}.{table_name}"
    col_count = len(df.columns)

    logger.info(f"Writing to {full_table_name} ({col_count} columns)")
    
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(full_table_name)

    try:
        row_count = spark.table(full_table_name).count()
    except Exception as e:
        logger.warning(f"Unable to fetch row_count for {full_table_name}: {e}")
        row_count = -1

    logger.info(f"Successfully wrote data to {full_table_name}")

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
    
    metadata = write_to_iceberg(spark, df, "payments")
    logger.info(f"Payments: {metadata['row_count']} rows (aggregated from multiple payment methods)")
    return df, metadata

def silver_cleaned_order_review(spark):
    """Clean and process order review data"""
    df = read_from_iceberg(spark, "olist_order_reviews_dataset")
    df = df.withColumn("review_score", col("review_score").cast("integer"))
    df = df.drop("review_comment_title")
    df = df.na.drop()
    
    metadata = write_to_iceberg(spark, df, "order_reviews")
    logger.info(f"Order Reviews: {metadata['row_count']} rows (1 per order)")
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
    """Clean and process geolocation data with Brazil boundaries filter and Aggregation"""
    df = read_from_iceberg(spark, "olist_geolocation_dataset")
    df = df.na.drop()
    

    df = df.filter(
        (col("geolocation_lat") <= 5.27438888)
        & (col("geolocation_lng") >= -73.98283055)
        & (col("geolocation_lat") >= -33.75116944)
        & (col("geolocation_lng") <= -34.79314722)
    )


    df_agg = df.groupBy("geolocation_zip_code_prefix").agg(
        avg("geolocation_lat").alias("geolocation_lat"),
        avg("geolocation_lng").alias("geolocation_lng"),
        first("geolocation_city").alias("geolocation_city"),
        first("geolocation_state").alias("geolocation_state")
    )
    
    metadata = write_to_iceberg(spark, df_agg, "geolocation")
    return df_agg, metadata

if __name__ == "__main__":
    load_dotenv()
    spark = get_spark_session("Silver Ingest")

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