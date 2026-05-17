import logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import mlflow.sklearn
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup import setup_mlflow
from src.mlops.data_loader        import load_anomaly_data
from src.mlops.p2_anomaly.model   import Autoencoder

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

ANOMALY_PERCENTILE = 95
EPOCHS             = 80
DEVICE             = torch.device("cuda" if torch.cuda.is_available() else "cpu")
EXPERIMENT         = "P2_Anomaly_Detection"

FEATURE_COLS = [
    "delivery_actual_days",
    "delivery_estimate_days",
    "total_freight_value",
    "total_product_value",
    "number_of_items",
]

AE_GRID = ParameterGrid({
    "lr":         [1e-3, 5e-4, 1e-4],
    "batch_size": [128, 256],
})


def train_autoencoder(X_tensor: torch.Tensor, X_val_tensor: torch.Tensor, lr: float,
                      batch_size: int) -> tuple:
    dataset    = TensorDataset(X_tensor)
    dataloader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

    model     = Autoencoder(input_size=len(FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr, weight_decay=1e-5)
    criterion = nn.MSELoss()
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(optimizer, T_max=EPOCHS)

    history    = []
    best_val_loss = float("inf")
    best_state = None
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

        model.eval()
        with torch.no_grad():
            val_loss = criterion(model(X_val_tensor.to(DEVICE)), X_val_tensor.to(DEVICE)).item()

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state = {k: v.cpu().clone() for k, v in model.state_dict().items()}

        if epoch % 10 == 0 or epoch == 1:
            logger.info(
                f"    Epoch {epoch:3d}/{EPOCHS} | "
                f"train_loss={avg_loss:.6f} | val_loss={val_loss:.6f}"
            )

    model.load_state_dict(best_state)
    model.eval()
    return model, history, best_val_loss


def compute_threshold(model: Autoencoder, X_tensor: torch.Tensor) -> tuple:
    model.eval()
    with torch.no_grad():
        errors = model.reconstruction_error(X_tensor.to(DEVICE)).cpu().numpy()
    threshold   = float(np.percentile(errors, ANOMALY_PERCENTILE))
    n_anomaly   = int((errors > threshold).sum())
    anomaly_pct = n_anomaly / len(errors) * 100
    logger.info(
        f"  [Threshold] P{ANOMALY_PERCENTILE}={threshold:.6f} "
        f"| Anomalies: {n_anomaly}/{len(errors)} ({anomaly_pct:.1f}%)"
    )
    return threshold, errors


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("P2 — Anomaly Detection (Autoencoder Grid Search)")
    logger.info("=" * 60)

    logger.info("\n[1/3] Loading data...")
    df = load_anomaly_data()
    logger.info(f"      {len(df):,} orders loaded")

    logger.info("\n[2/3] Preprocessing (StandardScaler, fit on train only)...")
    TRAIN_RATIO = 0.8
    split = int(len(df) * TRAIN_RATIO)
    scaler = Pipeline([
        ("log1p", FunctionTransformer(np.log1p, validate=False)),
        ("standard", StandardScaler()),
    ])
    X_train_raw = df[FEATURE_COLS].values[:split]
    X_val_raw   = df[FEATURE_COLS].values[split:]
    X_train_scaled = scaler.fit_transform(X_train_raw)
    X_val_scaled   = scaler.transform(X_val_raw)
    X_train_tensor = torch.tensor(X_train_scaled, dtype=torch.float32)
    X_val_tensor   = torch.tensor(X_val_scaled,   dtype=torch.float32)
    logger.info(f"      Train: {len(X_train_scaled):,} | Val: {len(X_val_scaled):,}")

    logger.info(f"\n[3/3] Autoencoder Grid Search ({len(AE_GRID)} configs) on {DEVICE}...")

    best_val_loss, best_cfg, best_model = float("inf"), None, None
    best_run_id = None

    with mlflow.start_run(run_name="Autoencoder_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")

        for i, cfg in enumerate(AE_GRID):
            run_name = f"ae_lr{cfg['lr']}_bs{cfg['batch_size']}"
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"\n  [{i+1}/{len(AE_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",        "autoencoder_pytorch")
                mlflow.log_param("input_size",         len(FEATURE_COLS))
                mlflow.log_param("bottleneck_size",    3)
                mlflow.log_param("epochs",             EPOCHS)
                mlflow.log_param("anomaly_percentile", ANOMALY_PERCENTILE)
                mlflow.log_param("n_train_samples",    len(X_train_scaled))
                mlflow.log_param("n_val_samples",      len(X_val_scaled))
                mlflow.log_param("features",           str(FEATURE_COLS))

                model, history, best_model_val_loss = train_autoencoder(
                    X_train_tensor, X_val_tensor, **cfg
                )

                for epoch, loss in enumerate(history, 1):
                    mlflow.log_metric("train_loss", loss, step=epoch)

                threshold, errors = compute_threshold(model, X_val_tensor)
                best_epoch_loss   = min(history)
                val_reconstruction_loss = float(errors.mean())
                mlflow.log_metric("reconstruction_error_mean",       float(errors.mean()))
                mlflow.log_metric("reconstruction_error_std",        float(errors.std()))
                mlflow.log_metric(f"threshold_p{ANOMALY_PERCENTILE}", threshold)
                mlflow.log_metric("anomaly_count",    int((errors > threshold).sum()))
                mlflow.log_metric("anomaly_rate_pct", float((errors > threshold).mean() * 100))
                mlflow.log_metric("best_epoch_loss",  best_epoch_loss)
                mlflow.log_metric("val_reconstruction_loss", val_reconstruction_loss)
                mlflow.log_metric("best_model_val_loss", best_model_val_loss)
                mlflow.pytorch.log_model(model, artifact_path="model")
                mlflow.sklearn.log_model(scaler, artifact_path="scaler")

                logger.info(f"    → Train Loss={best_epoch_loss:.6f} | Val Reconstruction={val_reconstruction_loss:.6f} | Threshold={threshold:.6f}")

                if val_reconstruction_loss < best_val_loss:
                    best_val_loss = val_reconstruction_loss
                    best_cfg    = cfg
                    best_model  = model
                    best_run_id = child.info.run_id

        mlflow.log_params({f"best_{k}": v for k, v in best_cfg.items()})
        mlflow.log_metric("best_val_reconstruction_loss", best_val_loss)
        logger.info(f"\n  ✅ Best: Val Reconstruction={best_val_loss:.6f} | Config={best_cfg}")

    mlflow.register_model(f"runs:/{best_run_id}/model", "anomaly_autoencoder")
    logger.info("  📦 Registered model: anomaly_autoencoder")
    logger.info("\n" + "=" * 60)
    logger.info("GRID SEARCH SUMMARY")
    logger.info(f"  Best Config      : {best_cfg}")
    logger.info(f"  Best Val Recon   : {best_val_loss:.6f}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem Nested Runs.")
