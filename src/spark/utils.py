import os
import logging
from pyspark.sql import SparkSession
from config.spark_config import SparkConnect

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session(app_name: str = "Lakehouse App") -> SparkSession:
    """
    Creates and returns a SparkSession with standard Lakehouse configurations.
    """
    jar_packages = [
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
        "spark.hadoop.fs.s3a.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.access.key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",

        # Map scheme `s3://` -> use S3A driver
        "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.AbstractFileSystem.s3.impl": "org.apache.hadoop.fs.s3a.S3A",
        "spark.hadoop.fs.s3.endpoint": os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        "spark.hadoop.fs.s3.path.style.access": "true",
        "spark.hadoop.fs.s3.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3.access.key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "spark.hadoop.fs.s3.secret.key": os.getenv("MINIO_SECRET_KEY", "minioadmin"),

        # Memory & Iceberg optimizations
        "spark.executor.memoryOverhead": "2g",
        "spark.sql.iceberg.direct-write.enabled": "false",
        "spark.sql.iceberg.vectorization.enabled": "false",
    }

    spark = SparkConnect(
        app_name=app_name,
        master_url="local[*]",
        executor_cores=2,
        executor_memory="4g",
        driver_memory="4g", # Adjusted for general use
        num_executors=1,
        jar_packages=jar_packages,
        spark_conf=spark_conf,
        log_level="WARN"
    ).spark
    
    return spark
