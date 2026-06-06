from __future__ import annotations

import math
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
from sklearn.metrics          import mean_squared_error, mean_absolute_error
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup  import setup_mlflow
from src.mlops.data_loader         import load_revenue_data
from src.mlops.revenue.model    import RevenueLSTM
from src.mlops.revenue.features import (
    TRAIN_RATIO,
    SEED,
    TARGET_COL,
    build_sequences,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EXPERIMENT    = "P1_Revenue_Forecasting"
EPOCHS        = 200
LSTM_PATIENCE = 30
DEVICE        = torch.device("cuda" if torch.cuda.is_available() else "cpu")

LSTM_GRID = ParameterGrid({
    "lr":            [1e-3, 5e-4],
    "batch_size":    [8, 16],
    "seq_len":       [4, 6],
    "hidden_size":   [64, 128],
    "num_layers":    [1],
    "dropout":       [0.1],
    "bidirectional": [False],
    "mlp_hidden":    [64],
})

def _set_seed(seed: int = SEED) -> None:
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def train_lstm(
    df:            pd.DataFrame,
    lr:            float,
    batch_size:    int,
    seq_len:       int,
    hidden_size:   int,
    num_layers:    int,
    dropout:       float,
    bidirectional: bool = False,
    mlp_hidden:    int  = 64,
) -> dict:
    _set_seed()
    X, y, feat_scaler, tgt_scaler, feature_cols = build_sequences(df, seq_len)
    split    = int(len(X) * TRAIN_RATIO)
    X_tr, X_val = X[:split], X[split:]
    y_tr, y_val = y[:split], y[split:]

    generator = torch.Generator().manual_seed(SEED)
    dl_train  = DataLoader(
        TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr)),
        batch_size=batch_size, shuffle=True, generator=generator,
    )
    dl_val = DataLoader(
        TensorDataset(torch.tensor(X_val), torch.tensor(y_val)),
        batch_size=batch_size,
    )

    model = RevenueLSTM(
        input_size    = len(feature_cols),
        hidden_size   = hidden_size,
        num_layers    = num_layers,
        dropout       = dropout,
        bidirectional = bidirectional,
        mlp_hidden    = mlp_hidden,
    ).to(DEVICE)

    optimizer = torch.optim.AdamW(model.parameters(), lr=lr, weight_decay=1e-4)
    criterion = nn.HuberLoss(delta=1.0)
    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=EPOCHS, eta_min=lr * 0.01
    )

    best_val_loss = float("inf")
    best_state    = None
    no_improve    = 0

    for epoch in range(1, EPOCHS + 1):
        model.train()
        for xb, yb in dl_train:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()

        model.eval()
        val_losses = []
        with torch.no_grad():
            for xb, yb in dl_val:
                xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                val_losses.append(criterion(model(xb), yb).item())
        val_loss = sum(val_losses) / len(val_losses)
        scheduler.step()

        if val_loss < best_val_loss - 1e-5:
            best_val_loss = val_loss
            best_state    = {k: v.cpu().clone() for k, v in model.state_dict().items()}
            no_improve    = 0
        else:
            no_improve += 1

        if epoch % 10 == 0:
            logger.info(f"    Epoch {epoch:3d}/{EPOCHS} | val_loss={val_loss:.6f}")
        if no_improve >= LSTM_PATIENCE:
            logger.info(f"    Early stopping at epoch {epoch} | best={best_val_loss:.6f}")
            break

    model.load_state_dict(best_state)
    model.eval()
    preds_norm, truth_norm = [], []
    with torch.no_grad():
        for xb, yb in dl_val:
            preds_norm.append(model(xb.to(DEVICE)).cpu().numpy())
            truth_norm.append(yb.numpy())

    preds_log  = tgt_scaler.inverse_transform(np.vstack(preds_norm))
    truth_log  = tgt_scaler.inverse_transform(np.vstack(truth_norm))
    preds_orig = np.maximum(np.expm1(preds_log), 0)
    truth_orig = np.maximum(np.expm1(truth_log), 0)
    rmse = math.sqrt(mean_squared_error(truth_orig, preds_orig))
    mae  = mean_absolute_error(truth_orig, preds_orig)

    return {
        "model":          model,
        "feat_scaler":    feat_scaler,
        "target_scaler":  tgt_scaler,
        "rmse":           rmse,
        "mae":            mae,
        "features":       feature_cols,
        "seq_len":        seq_len,
        "best_val_loss":  best_val_loss,
    }


def run_lstm_grid(df: pd.DataFrame) -> tuple[dict, dict, float]:
    best_rmse, best_cfg, best_result, best_run_id = float("inf"), None, None, None

    with mlflow.start_run(run_name="LSTM_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")
        for i, cfg in enumerate(LSTM_GRID):
            run_name = (
                f"lstm_lr{cfg['lr']}_bs{cfg['batch_size']}"
                f"_seq{cfg['seq_len']}_h{cfg['hidden_size']}"
                f"_bi{cfg['bidirectional']}"
            )
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"    [{i+1}/{len(LSTM_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",       "lstm_pytorch")
                mlflow.log_param("epochs",           EPOCHS)
                mlflow.log_param("patience",         LSTM_PATIENCE)
                mlflow.log_param("train_ratio",      TRAIN_RATIO)
                mlflow.log_param("optimizer",        "AdamW")
                mlflow.log_param("scheduler",        "CosineAnnealing")
                mlflow.log_param("target_transform", "log1p_standard_scaled")
                result = train_lstm(df, **cfg)
                mlflow.log_param("feature_count", len(result["features"]))
                mlflow.log_metric("rmse",          result["rmse"])
                mlflow.log_metric("mae",           result["mae"])
                mlflow.log_metric("best_val_loss", result["best_val_loss"])
                mlflow.pytorch.log_model(result["model"],         artifact_path="model")
                mlflow.sklearn.log_model(result["feat_scaler"],   artifact_path="feat_scaler")
                mlflow.sklearn.log_model(result["target_scaler"], artifact_path="target_scaler")
                logger.info(f"      → RMSE={result['rmse']:,.2f}  MAE={result['mae']:,.2f}")
                if result["rmse"] < best_rmse:
                    best_rmse, best_cfg, best_result = result["rmse"], cfg, result
                    best_run_id = child.info.run_id

        mlflow.log_params({f"best_{k}": v for k, v in best_cfg.items()})
        mlflow.log_metric("best_rmse", best_rmse)
        logger.info(f"\n  ✅ LSTM Best: RMSE={best_rmse:,.2f} | Config={best_cfg}")

    mlflow.register_model(f"runs:/{best_run_id}/model", "revenue_lstm")
    logger.info("  📦 Registered model: revenue_lstm")
    return best_result, best_cfg, best_rmse


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info(f"LSTM Grid Search — P1 Revenue Forecasting  [{DEVICE}]")
    logger.info("=" * 60)

    df = load_revenue_data().sort_values(["year", "week_of_year"]).reset_index(drop=True)
    logger.info(f"  {len(df)} weekly records loaded")

    result, cfg, rmse = run_lstm_grid(df)
    logger.info(f"\n  Champion RMSE: {rmse:,.2f} | Config: {cfg}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem Nested Runs.")
