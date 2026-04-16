import math, logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import mlflow.sklearn
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing   import StandardScaler, MinMaxScaler
from sklearn.metrics          import mean_squared_error, mean_absolute_error
import lightgbm as lgb
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup import setup_mlflow
from src.mlops.data_loader        import load_revenue_data
from src.mlops.p1_revenue.model   import RevenueLSTM

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SEQ_LEN     = 4
TRAIN_RATIO = 0.8
EPOCHS      = 60
DEVICE      = torch.device("cuda" if torch.cuda.is_available() else "cpu")

FEATURE_COLS = ["week_of_year", "month", "is_weekend", "order_count"]
TARGET_COL   = "weekly_revenue"
EXPERIMENT   = "P1_Revenue_Forecasting"

LGB_GRID = ParameterGrid({
    "learning_rate":    [0.01, 0.05],
    "num_leaves":       [15, 31],
    "min_data_in_leaf": [5, 10],
})

LSTM_GRID = ParameterGrid({
    "lr":         [1e-3, 5e-4],
    "batch_size": [16, 32],
})


def build_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for lag in [1, 2, 3, 4]:
        df[f"lag_{lag}"] = df[TARGET_COL].shift(lag)
    df["rolling_4"]     = df[TARGET_COL].shift(1).rolling(4).mean()
    df["rolling_4_std"] = df[TARGET_COL].shift(1).rolling(4).std()
    return df.dropna().reset_index(drop=True)


def build_sequences(df: pd.DataFrame, seq_len: int):
    feat_scaler   = MinMaxScaler()
    target_scaler = MinMaxScaler()
    X_raw = feat_scaler.fit_transform(df[FEATURE_COLS].values)
    y_raw = target_scaler.fit_transform(df[[TARGET_COL]].values)
    X_seq, y_seq = [], []
    for i in range(len(df) - seq_len):
        X_seq.append(X_raw[i: i + seq_len])
        y_seq.append(y_raw[i + seq_len])
    return (
        np.array(X_seq, dtype=np.float32),
        np.array(y_seq, dtype=np.float32),
        feat_scaler, target_scaler,
    )


def train_lightgbm(df: pd.DataFrame, learning_rate: float,
                   num_leaves: int, min_data_in_leaf: int) -> dict:
    df_lag = build_lag_features(df)
    lgb_features = FEATURE_COLS + [f"lag_{i}" for i in [1, 2, 3, 4]] + \
                   ["rolling_4", "rolling_4_std"]
    split = int(len(df_lag) * TRAIN_RATIO)
    X_train, X_val = df_lag[lgb_features][:split], df_lag[lgb_features][split:]
    y_train, y_val = df_lag[TARGET_COL][:split],   df_lag[TARGET_COL][split:]

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    params = {
        "objective": "regression", "metric": "rmse",
        "learning_rate": learning_rate,
        "num_leaves": num_leaves,
        "min_data_in_leaf": min_data_in_leaf,
        "verbose": -1,
    }
    lgb_train = lgb.Dataset(X_train_s, y_train)
    lgb_val   = lgb.Dataset(X_val_s,   y_val, reference=lgb_train)
    model = lgb.train(
        params, lgb_train, num_boost_round=300, valid_sets=[lgb_val],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(50)],
    )
    preds = model.predict(X_val_s)
    rmse  = math.sqrt(mean_squared_error(y_val, preds))
    mae   = mean_absolute_error(y_val, preds)
    return {"model": model, "scaler": scaler, "rmse": rmse, "mae": mae,
            "features": lgb_features}


def train_lstm(df: pd.DataFrame, lr: float, batch_size: int) -> dict:
    X, y, feat_scaler, target_scaler = build_sequences(df, SEQ_LEN)
    split = int(len(X) * TRAIN_RATIO)
    X_tr, X_val = X[:split], X[split:]
    y_tr, y_val = y[:split], y[split:]

    ds_train = TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr))
    ds_val   = TensorDataset(torch.tensor(X_val), torch.tensor(y_val))
    dl_train = DataLoader(ds_train, batch_size=batch_size, shuffle=True)
    dl_val   = DataLoader(ds_val,   batch_size=batch_size)

    model     = RevenueLSTM(input_size=len(FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    criterion = nn.MSELoss()
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=10, factor=0.5)

    best_val_loss = float("inf")
    best_state    = None
    for epoch in range(1, EPOCHS + 1):
        model.train()
        for xb, yb in dl_train:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            optimizer.step()

        model.eval()
        val_losses = []
        with torch.no_grad():
            for xb, yb in dl_val:
                xb, yb = xb.to(DEVICE), yb.to(DEVICE)
                val_losses.append(criterion(model(xb), yb).item())
        val_loss = sum(val_losses) / len(val_losses)
        scheduler.step(val_loss)
        if val_loss < best_val_loss:
            best_val_loss = val_loss
            best_state    = {k: v.cpu().clone() for k, v in model.state_dict().items()}
        if epoch % 10 == 0:
            logger.info(f"    Epoch {epoch:3d}/{EPOCHS} | val_loss={val_loss:.6f}")

    model.load_state_dict(best_state)
    model.eval()
    preds_norm, truth_norm = [], []
    with torch.no_grad():
        for xb, yb in dl_val:
            preds_norm.append(model(xb.to(DEVICE)).cpu().numpy())
            truth_norm.append(yb.numpy())
    preds_orig = target_scaler.inverse_transform(np.vstack(preds_norm))
    truth_orig = target_scaler.inverse_transform(np.vstack(truth_norm))
    rmse = math.sqrt(mean_squared_error(truth_orig, preds_orig))
    mae  = mean_absolute_error(truth_orig, preds_orig)
    return {"model": model, "feat_scaler": feat_scaler,
            "target_scaler": target_scaler, "rmse": rmse, "mae": mae}


def run_lightgbm_grid(df: pd.DataFrame) -> tuple:
    best_rmse, best_cfg, best_result, best_run_id = float("inf"), None, None, None

    with mlflow.start_run(run_name="LightGBM_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")
        for i, cfg in enumerate(LGB_GRID):
            run_name = f"lgb_lr{cfg['learning_rate']}_nl{cfg['num_leaves']}_md{cfg['min_data_in_leaf']}"
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"    [{i+1}/{len(LGB_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",  "lightgbm")
                mlflow.log_param("train_ratio", TRAIN_RATIO)
                result = train_lightgbm(df, **cfg)
                mlflow.log_metric("rmse", result["rmse"])
                mlflow.log_metric("mae",  result["mae"])
                mlflow.sklearn.log_model(result["model"], artifact_path="model")
                logger.info(f"      → RMSE={result['rmse']:,.2f}  MAE={result['mae']:,.2f}")
                if result["rmse"] < best_rmse:
                    best_rmse, best_cfg, best_result = result["rmse"], cfg, result
                    best_run_id = child.info.run_id

        mlflow.log_params({f"best_{k}": v for k, v in best_cfg.items()})
        mlflow.log_metric("best_rmse", best_rmse)
        logger.info(f"\n  ✅ LightGBM Best: RMSE={best_rmse:,.2f} | Config={best_cfg}")

    mlflow.register_model(f"runs:/{best_run_id}/model", "revenue_lightgbm")
    logger.info("  📦 Registered model: revenue_lightgbm")
    return best_result, best_cfg, best_rmse


def run_lstm_grid(df: pd.DataFrame) -> tuple:
    best_rmse, best_cfg, best_result, best_run_id = float("inf"), None, None, None

    with mlflow.start_run(run_name="LSTM_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")
        for i, cfg in enumerate(LSTM_GRID):
            run_name = f"lstm_lr{cfg['lr']}_bs{cfg['batch_size']}"
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"    [{i+1}/{len(LSTM_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",  "lstm_pytorch")
                mlflow.log_param("seq_len",     SEQ_LEN)
                mlflow.log_param("hidden_size", 64)
                mlflow.log_param("num_layers",  2)
                mlflow.log_param("epochs",      EPOCHS)
                mlflow.log_param("train_ratio", TRAIN_RATIO)
                result = train_lstm(df, **cfg)
                mlflow.log_metric("rmse", result["rmse"])
                mlflow.log_metric("mae",  result["mae"])
                mlflow.pytorch.log_model(result["model"], artifact_path="model")
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
    logger.info("P1 — Revenue Forecasting (Hyperparameter Grid Search)")
    logger.info("=" * 60)

    logger.info("\n[1/3] Loading data from Iceberg Gold...")
    df = load_revenue_data()
    logger.info(f"      {len(df)} weekly records | date range: "
                f"{df['year'].min()}-W{df['week_of_year'].min()} → "
                f"{df['year'].max()}-W{df['week_of_year'].max()}")

    logger.info(f"\n[2/3] LightGBM Grid Search ({len(LGB_GRID)} configs)...")
    lgb_result, lgb_best_cfg, lgb_best_rmse = run_lightgbm_grid(df)

    logger.info(f"\n[3/3] LSTM Grid Search ({len(LSTM_GRID)} configs) on {DEVICE}...")
    lstm_result, lstm_best_cfg, lstm_best_rmse = run_lstm_grid(df)

    logger.info("\n" + "=" * 60)
    logger.info("GRID SEARCH SUMMARY")
    logger.info(f"  LightGBM Best RMSE : {lgb_best_rmse:,.2f} | Params: {lgb_best_cfg}")
    logger.info(f"  LSTM     Best RMSE : {lstm_best_rmse:,.2f} | Params: {lstm_best_cfg}")
    winner      = "LightGBM" if lgb_best_rmse < lstm_best_rmse else "LSTM"
    winner_cfg  = lgb_best_cfg if winner == "LightGBM" else lstm_best_cfg
    logger.info(f"  → Champion Model  : {winner} | Config: {winner_cfg}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem Nested Runs.")
