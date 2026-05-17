import os
import logging
import random
import numpy as np
import torch
import mlflow
import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")

def set_seed(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed(seed)
        torch.cuda.manual_seed_all(seed)
        torch.backends.cudnn.deterministic = True
        torch.backends.cudnn.benchmark = False
    log.info(f"[Seed] Đã thiết lập Global Random Seed = {seed}")


def _ensure_minio_bucket(bucket_name: str = "mlflow") -> None:
    endpoint = f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
    )
    try:
        s3.head_bucket(Bucket=bucket_name)
        log.info("[MinIO] Bucket '%s' đã tồn tại.", bucket_name)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("404", "NoSuchBucket"):
            s3.create_bucket(Bucket=bucket_name)
            log.info("[MinIO] Đã tạo bucket '%s'.", bucket_name)
            print(f"[MinIO] Bucket '{bucket_name}' vừa được tạo.")
        else:
            raise


def setup_mlflow(experiment_name: str) -> str:
    set_seed(42)  # Cố định seed cho toàn bộ MLOps
    os.environ["AWS_ACCESS_KEY_ID"]     = os.getenv("MINIO_ACCESS_KEY", "minio")
    os.environ["AWS_SECRET_ACCESS_KEY"] = os.getenv("MINIO_SECRET_KEY", "minio123")
    os.environ["MLFLOW_S3_ENDPOINT_URL"] = (
        f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}"
    )
    os.environ["MLFLOW_S3_IGNORE_TLS"] = "true"

    _ensure_minio_bucket("mlflow")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(experiment_name)

    client = mlflow.tracking.MlflowClient()
    exp    = client.get_experiment_by_name(experiment_name)
    print(f"[MLflow] Experiment '{experiment_name}' (id={exp.experiment_id}) @ {MLFLOW_TRACKING_URI}")
    return exp.experiment_id
