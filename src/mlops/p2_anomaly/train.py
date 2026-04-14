import logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.preprocessing import StandardScaler
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup import setup_mlflow
from src.mlops.data_loader        import load_anomaly_data
from src.mlops.p2_anomaly.model   import Autoencoder

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EPOCHS          = 80
LR              = 1e-3
BATCH_SIZE      = 256
ANOMALY_PERCENTILE = 95
DEVICE          = torch.device("cuda" if torch.cuda.is_available() else "cpu")
EXPERIMENT      = "P2_Anomaly_Detection"

FEATURE_COLS = [
    "delivery_actual_days",
    "delivery_estimate_days",
    "total_freight_value",
    "total_product_value",
    "number_of_items",
]


def train_autoencoder(X_tensor: torch.Tensor) -> tuple[Autoencoder, list[float]]:
    dataset    = TensorDataset(X_tensor)
    dataloader = DataLoader(dataset, batch_size=BATCH_SIZE, shuffle=True)

    model     = Autoencoder(input_size=len(FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR, weight_decay=1e-5)
    criterion = nn.MSELoss()
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)

    history = []
    for epoch in range(1, EPOCHS + 1):
        model.train()
        epoch_losses = []
        for (xb,) in dataloader:
            xb = xb.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(xb), xb)
            loss.backward()
            optimizer.step()
            epoch_losses.append(loss.item())

        avg_loss = sum(epoch_losses) / len(epoch_losses)
        scheduler.step()
        history.append(avg_loss)

        if epoch % 10 == 0 or epoch == 1:
            logger.info(f"  Epoch {epoch:3d}/{EPOCHS} | loss={avg_loss:.6f}")

    return model, history


def compute_threshold(model: Autoencoder, X_tensor: torch.Tensor) -> float:
    model.eval()
    with torch.no_grad():
        errors = model.reconstruction_error(X_tensor.to(DEVICE)).cpu().numpy()
    threshold = float(np.percentile(errors, ANOMALY_PERCENTILE))
    n_anomaly = int((errors > threshold).sum())
    anomaly_pct = n_anomaly / len(errors) * 100
    logger.info(
        f"[Threshold] P{ANOMALY_PERCENTILE} = {threshold:.6f} "
        f"| Anomalies: {n_anomaly}/{len(errors)} ({anomaly_pct:.1f}%)"
    )
    return threshold, errors


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("P2 — Anomaly Detection (Autoencoder)")
    logger.info("=" * 60)

    logger.info("\n[1/3] Loading data...")
    df = load_anomaly_data()
    logger.info(f"      {len(df):,} orders loaded")

    logger.info("\n[2/3] Preprocessing (StandardScaler)...")
    scaler  = StandardScaler()
    X_scaled = scaler.fit_transform(df[FEATURE_COLS].values)
    X_tensor = torch.tensor(X_scaled, dtype=torch.float32)

    stats = df[FEATURE_COLS].describe()
    logger.info(f"\n{stats.to_string()}")

    logger.info(f"\n[3/3] Training Autoencoder on {DEVICE}...")
    with mlflow.start_run(run_name="Autoencoder"):
        mlflow.log_param("model_type",          "autoencoder_pytorch")
        mlflow.log_param("input_size",           len(FEATURE_COLS))
        mlflow.log_param("bottleneck_size",      3)
        mlflow.log_param("epochs",               EPOCHS)
        mlflow.log_param("batch_size",           BATCH_SIZE)
        mlflow.log_param("lr",                   LR)
        mlflow.log_param("anomaly_percentile",   ANOMALY_PERCENTILE)
        mlflow.log_param("n_samples",            len(df))
        mlflow.log_param("features",             str(FEATURE_COLS))

        model, history = train_autoencoder(X_tensor)

        for epoch, loss in enumerate(history, 1):
            mlflow.log_metric("train_loss", loss, step=epoch)

        threshold, errors = compute_threshold(model, X_tensor)
        mlflow.log_metric("reconstruction_error_mean",      float(errors.mean()))
        mlflow.log_metric("reconstruction_error_std",       float(errors.std()))
        mlflow.log_metric(f"threshold_p{ANOMALY_PERCENTILE}", threshold)
        mlflow.log_metric("anomaly_count",   int((errors > threshold).sum()))
        mlflow.log_metric("anomaly_rate_pct", float((errors > threshold).mean() * 100))
        mlflow.log_metric("final_train_loss", history[-1])

        mlflow.pytorch.log_model(
            model,
            artifact_path="model",
            registered_model_name="anomaly_autoencoder",
        )

        import json, tempfile, os
        meta = {"threshold": threshold, "percentile": ANOMALY_PERCENTILE,
                "feature_cols": FEATURE_COLS}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(meta, f)
            tmp_path = f.name
        mlflow.log_artifact(tmp_path, artifact_path="metadata")
        os.unlink(tmp_path)

        logger.info("\n✅ Autoencoder training complete!")
        logger.info(f"   Final loss      : {history[-1]:.6f}")
        logger.info(f"   Anomaly threshold: {threshold:.6f}")
        logger.info("   Mở MLflow UI tại http://localhost:5000 để xem.")
