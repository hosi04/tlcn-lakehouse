from pyspark.sql import DataFrame
from pyspark.sql.functions import *
import logging
from dotenv import load_dotenv
from src.spark.utils import get_spark_session

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

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


def dim_customer(spark):
    logger.info("Building dim_customer ...")
    df_customers = read_from_iceberg(spark, "customers")
    df_geolocation = read_from_iceberg(spark, "geolocation")
    
    df = df_customers.join(
        df_geolocation, 
        df_customers["customer_zip_code_prefix"] == df_geolocation["geolocation_zip_code_prefix"],
        how="left"
    )
    
    df = (df
        .withColumnRenamed("geolocation_lat", "customer_lat")
        .withColumnRenamed("geolocation_lng", "customer_lng")
        .withColumnRenamed("customer_id", "customer_id")
        .withColumnRenamed("customer_unique_id", "customer_unique_id")
    )
    
    df = df.select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_lat",
        "customer_lng"
    ).dropDuplicates(subset=["customer_id"])
    
    df = df.withColumn("customer_key", monotonically_increasing_id())


    df = df.select(
        "customer_key",
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_lat",
        "customer_lng"
    )


    metadata = write_to_iceberg(spark, df, "dim_customer")
    return df, metadata

def dim_product(spark):
    logger.info("Building dim_product ...")
    
    df_products = read_from_iceberg(spark, "products").alias("p")
    df_product_category = read_from_iceberg(spark, "product_category").alias("pc")
    
    
    df = df_products.join(
        df_product_category,
        col("p.product_category_name") == col("pc.product_category_name"),
        "left"
    )
    
    df = (df.select(
            col("p.product_id").alias("product_id"),
            col("pc.product_category_name").alias("product_category_name"),
            col("pc.product_category_name_english"),
            col("p.product_name_length"),
            col("p.product_description_length"),
            col("p.product_photos_qty"),
            col("p.product_weight_g"),
            col("p.product_length_cm"),
            col("p.product_height_cm"),
            col("p.product_width_cm")
        )
    )


    df = df.withColumn("product_key", monotonically_increasing_id())
    
    df = df.select(
        "product_key",
        "product_id",
        "product_category_name",
        "product_category_name_english",
        "product_name_length",
        "product_description_length",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    )
    
    metadata = write_to_iceberg(spark, df, "dim_product")
    return df, metadata

def dim_seller(spark):
    logger.info("Building dim_seller ...")
    df_sellers = read_from_iceberg(spark, "sellers")
    
    
    df = (df_sellers.select(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        )
    )
    
    df = df.withColumn("seller_key", monotonically_increasing_id())
    
    df = df.select(
        "seller_key",
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state"
    )

    metadata = write_to_iceberg(spark, df, "dim_seller")
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
        .withColumn("date_key", 
            year(col("date")) * 10000 + 
            month(col("date")) * 100 + 
            dayofmonth(col("date"))
        )
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

def fact_order_item(spark):
    logger.info("Building fact_order_item ...")
    
    df_orders = read_from_iceberg(spark, "orders").alias("o")
    df_order_items = read_from_iceberg(spark, "order_items").alias("oi")
    
    dim_customer = read_from_iceberg(spark, "dim_customer", namespace="iceberg.gold").alias("c")
    dim_product = read_from_iceberg(spark, "dim_product", namespace="iceberg.gold").alias("p")
    dim_seller = read_from_iceberg(spark, "dim_seller", namespace="iceberg.gold").alias("s")
    dim_date = read_from_iceberg(spark, "dim_date", namespace="iceberg.gold")
    
    df_base = df_orders.join(df_order_items, col("o.order_id") == col("oi.order_id"), "inner")
    
    df_base = df_base.withColumn(
        "shipping_days",
        when(
            col("o.order_delivered_customer_date").isNotNull(),
            datediff(col("o.order_delivered_customer_date"), col("o.order_purchase_timestamp"))
        ).otherwise(None)
    )
    
    df = (df_base
        .join(dim_customer, col("o.customer_id") == col("c.customer_id"), "inner")
        .join(dim_product, col("oi.product_id") == col("p.product_id"), "inner")
        .join(dim_seller, col("oi.seller_id") == col("s.seller_id"), "inner")
        .join(
            dim_date.alias("purchase_date"),
            col("o.order_purchase_timestamp").cast("date") == col("purchase_date.date"),
            "inner"
        )
        .join(
            dim_date.alias("delivered_date"),
            col("o.order_delivered_customer_date").cast("date") == col("delivered_date.date"),
            "left"
        )
    )
    
    df = df.select(
        col("o.order_id").alias("order_id"),
        col("oi.order_item_id").alias("order_item_id"),
        col("c.customer_key"),
        col("p.product_key"),
        col("s.seller_key"),
        col("purchase_date.date_key").alias("purchase_date_key"),
        col("delivered_date.date_key").alias("delivered_date_key"),
        col("o.order_status"),
        col("oi.price").alias("item_price"),
        col("oi.freight_value").alias("item_freight_value"),
        (col("oi.price") + col("oi.freight_value")).alias("item_total_value"),
        col("shipping_days")
    )

    df = df.withColumn("order_item_key", monotonically_increasing_id())
    
    df = df.select(
        "order_item_key",
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "seller_key",
        "purchase_date_key",
        "delivered_date_key",
        "order_status",
        "item_price",
        "item_freight_value",
        "item_total_value",
        "shipping_days"
    )
    
    metadata = write_to_iceberg(spark, df, "fact_order_item")
    return df, metadata

def fact_order(spark):
    logger.info("Building fact_order ...")
    
    df_orders = read_from_iceberg(spark, "orders").alias("o")
    df_order_items = read_from_iceberg(spark, "order_items").alias("oi")
    df_payments = read_from_iceberg(spark, "payments").alias("pay")
    dim_customer = read_from_iceberg(spark, "dim_customer", namespace="iceberg.gold").alias("c")
    dim_date = read_from_iceberg(spark, "dim_date", namespace="iceberg.gold")
    
    df_order_agg = df_order_items.groupBy(col("oi.order_id")).agg(
        count(col("oi.order_item_id")).alias("number_of_items"),
        sum(col("oi.price")).alias("total_product_value"),
        sum(col("oi.freight_value")).alias("total_freight_value")
    ).alias("agg_items")
    
    df_payment_agg = df_payments.groupBy(col("pay.order_id")).agg(
        sum(col("pay.payment_value")).alias("total_payment_value"),
        sum(col("pay.payment_installments")).alias("total_installments"),
        count(col("pay.payment_sequential")).alias("payment_count"),
        first(col("pay.payment_type")).alias("primary_payment_type")
    ).alias("agg_pay")
    
    df_orders_calc = df_orders.withColumn(
        "delivery_actual_days",
        when(
            col("o.order_delivered_customer_date").isNotNull(),
            datediff(col("o.order_delivered_customer_date"), col("o.order_purchase_timestamp"))
        ).otherwise(None)
    ).withColumn(
        "delivery_estimate_days",
        when(
            col("o.order_estimated_delivery_date").isNotNull(),
            datediff(col("o.order_estimated_delivery_date"), col("o.order_purchase_timestamp"))
        ).otherwise(None)
    ).withColumn(
        "delivery_early_days",
        when(
            col("o.order_delivered_customer_date").isNotNull() & col("o.order_estimated_delivery_date").isNotNull(),
            datediff(col("o.order_estimated_delivery_date"), col("o.order_delivered_customer_date"))
        ).otherwise(None)
    )
    
    df = (df_orders_calc
        .join(df_order_agg, col("o.order_id") == col("agg_items.order_id"), "inner")
        .join(df_payment_agg, col("o.order_id") == col("agg_pay.order_id"), "inner")
        .join(dim_customer, col("o.customer_id") == col("c.customer_id"), "inner")
        .join(
            dim_date.alias("purchase_date"),
            col("o.order_purchase_timestamp").cast("date") == col("purchase_date.date"),
            "inner"
        )
        .join(
            dim_date.alias("delivered_date"),
            col("o.order_delivered_customer_date").cast("date") == col("delivered_date.date"),
            "left"
        )
        .join(
            dim_date.alias("estimated_date"),
            col("o.order_estimated_delivery_date").cast("date") == col("estimated_date.date"),
            "left"
        )
    )
    
    df = df.select(
        col("o.order_id").alias("order_id"),
        col("c.customer_key"),
        col("purchase_date.date_key").alias("purchase_date_key"),
        col("delivered_date.date_key").alias("delivered_date_key"),
        col("estimated_date.date_key").alias("estimated_delivery_date_key"),
        col("o.order_status"),
        col("agg_pay.primary_payment_type"),
        col("agg_pay.total_payment_value"),
        col("agg_items.total_product_value"),
        col("agg_items.total_freight_value"),
        col("agg_items.number_of_items"),
        col("agg_pay.total_installments"),
        col("agg_pay.payment_count"),
        col("delivery_actual_days"),
        col("delivery_estimate_days"),
        col("delivery_early_days")
    )
    
    df = df.withColumn("order_key", monotonically_increasing_id())
    
    df = df.select(
        "order_key",
        "order_id",
        "customer_key",
        "purchase_date_key",
        "delivered_date_key",
        "estimated_delivery_date_key",
        "order_status",
        "primary_payment_type",
        "total_payment_value",
        "total_product_value",
        "total_freight_value",
        "number_of_items",
        "total_installments",
        "payment_count",
        "delivery_actual_days",
        "delivery_estimate_days",
        "delivery_early_days"
    )

    metadata = write_to_iceberg(spark, df, "fact_order")
    return df, metadata

if __name__ == "__main__":
    load_dotenv()
    spark = get_spark_session("Gold Ingest")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    
    logger.info("=" * 60)
    logger.info("Starting Gold layer processing (Star Schema)")
    logger.info("=" * 60)
    
    try:
        logger.info("\n--- Building Dimensions ---")
        dim_customer(spark)
        dim_product(spark)
        dim_seller(spark)
        dim_date(spark)
        
        logger.info("\n--- Building Fact Tables ---")
        fact_order_item(spark)
        fact_order(spark)
        
        logger.info("\n" + "=" * 60)
        logger.info("Gold layer processing completed successfully!")


        
    except Exception as e:
        logger.error(f"Error during Gold layer processing: {e}")
        raise
    finally:
        spark.stop()