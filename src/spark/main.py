from config.spark_config import SparkConnect
from dotenv import load_dotenv
import os
def main():
    load_dotenv()
    jar_packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0",
        "software.amazon.awssdk:s3:2.20.125",
        "org.apache.hadoop:hadoop-aws:3.3.6"
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
        app_name="thanhdz",
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="2g",
        num_executors=3,
        jar_packages=jar_packages,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark
    print("Spark Session have been created successfully.")

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS"))
        .option("subscribe", os.getenv("KAFKA_TOPIC"))
        .option("startingOffsets", "earliest")
        .load()
    )

    raw_data_df = kafka_stream_df.selectExpr("CAST(value AS STRING) AS raw_json")

    query = (
        raw_data_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .option("checkpointLocation", os.getenv("CHECKPOINT_LOCATION"))
        .toTable(os.getenv("ICEBERG_TABLE_NAME"))
    )
    print("Start streaming......")

    query.awaitTermination()

if __name__ == "__main__":
    main()
