import argparse
import logging
from pathlib import Path

import mlflow
import mlflow.pytorch
import mlflow.sklearn
import pandas as pd
import torch
from dotenv import load_dotenv
from mlflow.tracking import MlflowClient
from pyspark.sql.functions import col, get_json_object

from src.spark.utils import get_spark_session
from src.streaming.config import (
    ANOMALY_PREDICTIONS_TABLE,
    KAFKA_BOOTSTRAP_SERVERS,
    ORDER_EVENTS_TOPIC,
    STREAM_CHECKPOINT_ROOT,
)


logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

FEATURE_COLS = [
    "delivery_actual_days",
    "delivery_estimate_days",
    "total_freight_value",
    "total_product_value",
    "number_of_items",
]


def _latest_model_version(client: MlflowClient, model_name: str):
    versions = client.search_model_versions(f"name = '{model_name}'")
    if not versions:
        raise RuntimeError(f"No registered versions found for model: {model_name}")
    return sorted(versions, key=lambda version: int(version.version), reverse=True)[0]


def load_anomaly_artifacts(model_name: str, threshold_metric: str):
    client = MlflowClient()
    version = _latest_model_version(client, model_name)
    run_id = version.run_id
    model_uri = f"models:/{model_name}/{version.version}"
    scaler_uri = f"runs:/{run_id}/scaler"

    model = mlflow.pytorch.load_model(model_uri)
    scaler = mlflow.sklearn.load_model(scaler_uri)
    run = client.get_run(run_id)
    threshold = run.data.metrics.get(threshold_metric)
    if threshold is None:
        raise RuntimeError(f"Missing MLflow metric '{threshold_metric}' in run {run_id}")

    model.eval()
    logger.info(
        "Loaded anomaly artifacts: model=%s version=%s run_id=%s threshold=%s",
        model_name,
        version.version,
        run_id,
        threshold,
    )
    return model, scaler, float(threshold)


def _score_batch(batch_df, batch_id: int, model, scaler, threshold: float, output_table: str):
    if batch_df.rdd.isEmpty():
        return

    pdf = batch_df.toPandas()
    if pdf.empty:
        return

    purchase_ts = pd.to_datetime(pdf["order_purchase_timestamp"], errors="coerce")
    delivered_ts = pd.to_datetime(pdf["order_delivered_customer_date"], errors="coerce")
    estimated_ts = pd.to_datetime(pdf["order_estimated_delivery_date"], errors="coerce")

    pdf["delivery_actual_days"] = (delivered_ts - purchase_ts).dt.days
    pdf["delivery_estimate_days"] = (estimated_ts - purchase_ts).dt.days

    for col_name in ["total_freight_value", "total_product_value", "number_of_items"]:
        pdf[col_name] = pd.to_numeric(pdf[col_name], errors="coerce")

    feature_pdf = pdf.dropna(subset=FEATURE_COLS).copy()
    if feature_pdf.empty:
        logger.info("Batch %s has no valid rows after feature filtering.", batch_id)
        return

    scaled = scaler.transform(feature_pdf[FEATURE_COLS].values)
    tensor = torch.tensor(scaled, dtype=torch.float32)
    with torch.no_grad():
        errors = model.reconstruction_error(tensor).numpy()

    output_pdf = feature_pdf[
        [
            "event_id",
            "order_id",
            "seller_id",
            "event_time",
            "order_status",
        ]
    ].copy()
    output_pdf["reconstruction_error"] = errors
    output_pdf["threshold"] = threshold
    output_pdf["is_anomaly"] = errors > threshold
    output_pdf["scored_batch_id"] = int(batch_id)
    output_pdf["scored_at"] = pd.Timestamp.utcnow().isoformat()

    spark = batch_df.sparkSession
    spark.createDataFrame(output_pdf).write.format("iceberg").mode("append").saveAsTable(
        output_table
    )
    logger.info("Scored batch %s: %s predictions", batch_id, len(output_pdf))


def start_anomaly_inference_stream(
    bootstrap_servers: str,
    topic: str,
    output_table: str,
    checkpoint_location: Path,
    starting_offsets: str,
    processing_time: str,
    model_name: str,
    threshold_metric: str,
) -> None:
    load_dotenv()
    model, scaler, threshold = load_anomaly_artifacts(model_name, threshold_metric)

    spark = get_spark_session("Anomaly Inference Stream")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    event_json = col("value").cast("string")
    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
        .select(
            get_json_object(event_json, "$.event_id").alias("event_id"),
            get_json_object(event_json, "$.event_time").alias("event_time"),
            get_json_object(event_json, "$.order_id").alias("order_id"),
            get_json_object(event_json, "$.seller_id").alias("seller_id"),
            get_json_object(event_json, "$.order_status").alias("order_status"),
            get_json_object(event_json, "$.order_purchase_timestamp").alias(
                "order_purchase_timestamp"
            ),
            get_json_object(event_json, "$.order_delivered_customer_date").alias(
                "order_delivered_customer_date"
            ),
            get_json_object(event_json, "$.order_estimated_delivery_date").alias(
                "order_estimated_delivery_date"
            ),
            get_json_object(event_json, "$.total_freight_value").alias(
                "total_freight_value"
            ),
            get_json_object(event_json, "$.total_product_value").alias(
                "total_product_value"
            ),
            get_json_object(event_json, "$.number_of_items").alias("number_of_items"),
        )
    )

    query = (
        stream_df.writeStream.foreachBatch(
            lambda batch_df, batch_id: _score_batch(
                batch_df=batch_df,
                batch_id=batch_id,
                model=model,
                scaler=scaler,
                threshold=threshold,
                output_table=output_table,
            )
        )
        .option("checkpointLocation", str(checkpoint_location))
        .trigger(processingTime=processing_time)
        .start()
    )

    logger.info("Started anomaly inference stream -> %s", output_table)
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run realtime anomaly inference from Kafka order events."
    )
    parser.add_argument("--bootstrap-servers", default=KAFKA_BOOTSTRAP_SERVERS)
    parser.add_argument("--topic", default=ORDER_EVENTS_TOPIC)
    parser.add_argument("--output-table", default=ANOMALY_PREDICTIONS_TABLE)
    parser.add_argument(
        "--checkpoint-location",
        type=Path,
        default=Path(STREAM_CHECKPOINT_ROOT) / "anomaly_inference_stream",
    )
    parser.add_argument("--starting-offsets", default="latest", choices=["earliest", "latest"])
    parser.add_argument("--processing-time", default="10 seconds")
    parser.add_argument("--model-name", default="anomaly_autoencoder")
    parser.add_argument("--threshold-metric", default="threshold_p95")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    start_anomaly_inference_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        output_table=args.output_table,
        checkpoint_location=args.checkpoint_location,
        starting_offsets=args.starting_offsets,
        processing_time=args.processing_time,
        model_name=args.model_name,
        threshold_metric=args.threshold_metric,
    )
