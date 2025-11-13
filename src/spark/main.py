from config.spark_config import SparkConnect
from dotenv import load_dotenv
import os
import glob

def main():
    load_dotenv()
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

    csv_folder = "./data/"
    csv_files = glob.glob(os.path.join(csv_folder, "*.csv"))
    print(f"Found {len(csv_files)} CSV files.")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.bronze")

    for file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(file_path))[0]
        full_table_name = f"iceberg.bronze.{table_name}"
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(file_path)
        df.write.format("iceberg").mode("overwrite").saveAsTable(full_table_name)
        print(f"Data from {file_path} written to {full_table_name} ({df.count()} rows)")

    print("All CSV files have been uploaded to Iceberg successfully.")

    spark.stop()

if __name__ == "__main__":
    main()
