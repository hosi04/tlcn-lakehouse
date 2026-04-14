import math, logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import mlflow.sklearn
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
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

SEQ_LEN    = 4
TRAIN_RATIO= 0.8
EPOCHS     = 60
LR         = 1e-3
BATCH_SIZE = 16
DEVICE     = torch.device("cuda" if torch.cuda.is_available() else "cpu")

FEATURE_COLS = ["week_of_year", "month", "is_weekend", "order_count"]
TARGET_COL   = "weekly_revenue"
EXPERIMENT   = "P1_Revenue_Forecasting"


def build_sequences(df: pd.DataFrame, seq_len: int):
    feat_scaler   = MinMaxScaler()
    target_scaler = MinMaxScaler()

    X_raw = feat_scaler.fit_transform(df[FEATURE_COLS].values)
    y_raw = target_scaler.fit_transform(df[[TARGET_COL]].values)

    X_seq, y_seq = [], []
    for i in range(len(df) - seq_len):
        X_seq.append(X_raw[i : i + seq_len])
        y_seq.append(y_raw[i + seq_len])

    return (
        np.array(X_seq, dtype=np.float32),
        np.array(y_seq, dtype=np.float32),
        feat_scaler,
        target_scaler,
    )


def build_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for lag in [1, 2, 3, 4]:
        df[f"lag_{lag}"] = df[TARGET_COL].shift(lag)
    df["rolling_4"]  = df[TARGET_COL].shift(1).rolling(4).mean()
    df["rolling_4_std"] = df[TARGET_COL].shift(1).rolling(4).std()
    return df.dropna().reset_index(drop=True)


def train_lightgbm(df: pd.DataFrame) -> dict:
    df_lag = build_lag_features(df)
    lgb_features = FEATURE_COLS + [f"lag_{i}" for i in [1,2,3,4]] + ["rolling_4", "rolling_4_std"]

    split = int(len(df_lag) * TRAIN_RATIO)
    X_train, X_val = df_lag[lgb_features][:split], df_lag[lgb_features][split:]
    y_train, y_val = df_lag[TARGET_COL][:split],   df_lag[TARGET_COL][split:]

    scaler = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    params = {
        "objective": "regression",
        "metric":    "rmse",
        "learning_rate": 0.05,
        "num_leaves":    15,
        "min_data_in_leaf": 5,
        "verbose": -1,
    }

    lgb_train = lgb.Dataset(X_train_s, y_train)
    lgb_val   = lgb.Dataset(X_val_s,   y_val, reference=lgb_train)

    model = lgb.train(
        params,
        lgb_train,
        num_boost_round=300,
        valid_sets=[lgb_val],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(50)],
    )

    preds = model.predict(X_val_s)
    rmse  = math.sqrt(mean_squared_error(y_val, preds))
    mae   = mean_absolute_error(y_val, preds)
    logger.info(f"[LightGBM] RMSE={rmse:,.2f}  MAE={mae:,.2f}")

    return {"model": model, "scaler": scaler, "rmse": rmse, "mae": mae,
            "features": lgb_features}


def train_lstm(df: pd.DataFrame) -> dict:
    X, y, feat_scaler, target_scaler = build_sequences(df, SEQ_LEN)

    split      = int(len(X) * TRAIN_RATIO)
    X_tr, X_val = X[:split], X[split:]
    y_tr, y_val = y[:split], y[split:]

    ds_train = TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr))
    ds_val   = TensorDataset(torch.tensor(X_val), torch.tensor(y_val))
    dl_train = DataLoader(ds_train, batch_size=BATCH_SIZE, shuffle=True)
    dl_val   = DataLoader(ds_val,   batch_size=BATCH_SIZE)

    model     = RevenueLSTM(input_size=len(FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
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
            logger.info(f"  Epoch {epoch:3d}/{EPOCHS} | val_loss={val_loss:.6f}")

    model.load_state_dict(best_state)

    model.eval()
    preds_norm = []
    truth_norm = []
    with torch.no_grad():
        for xb, yb in dl_val:
            preds_norm.append(model(xb.to(DEVICE)).cpu().numpy())
            truth_norm.append(yb.numpy())

    preds_orig = target_scaler.inverse_transform(np.vstack(preds_norm))
    truth_orig = target_scaler.inverse_transform(np.vstack(truth_norm))
    rmse = math.sqrt(mean_squared_error(truth_orig, preds_orig))
    mae  = mean_absolute_error(truth_orig, preds_orig)
    logger.info(f"[LSTM] RMSE={rmse:,.2f}  MAE={mae:,.2f}")

    return {"model": model, "feat_scaler": feat_scaler,
            "target_scaler": target_scaler, "rmse": rmse, "mae": mae}


def log_lightgbm_run(result: dict) -> mlflow.ActiveRun:
    with mlflow.start_run(run_name="LightGBM") as run:
        mlflow.log_param("model_type",  "lightgbm")
        mlflow.log_param("seq_len",     "n/a — lag features")
        mlflow.log_param("train_ratio", TRAIN_RATIO)
        mlflow.log_metric("rmse", result["rmse"])
        mlflow.log_metric("mae",  result["mae"])
        mlflow.sklearn.log_model(result["model"], artifact_path="model",
                                 registered_model_name="revenue_lightgbm")
        logger.info(f"[MLflow] LightGBM run logged: {run.info.run_id}")
    return run


def log_lstm_run(result: dict) -> mlflow.ActiveRun:
    with mlflow.start_run(run_name="LSTM") as run:
        mlflow.log_param("model_type",  "lstm_pytorch")
        mlflow.log_param("seq_len",     SEQ_LEN)
        mlflow.log_param("hidden_size", 64)
        mlflow.log_param("num_layers",  2)
        mlflow.log_param("epochs",      EPOCHS)
        mlflow.log_param("lr",          LR)
        mlflow.log_metric("rmse", result["rmse"])
        mlflow.log_metric("mae",  result["mae"])
        mlflow.pytorch.log_model(result["model"], artifact_path="model",
                                 registered_model_name="revenue_lstm")
        logger.info(f"[MLflow] LSTM run logged: {run.info.run_id}")
    return run


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("P1 — Revenue Forecasting")
    logger.info("=" * 60)

    logger.info("\n[1/2] Loading data from Iceberg Gold...")
    df = load_revenue_data()
    logger.info(f"      {len(df)} weekly records | date range: "
                f"{df['year'].min()}-W{df['week_of_year'].min()} → "
                f"{df['year'].max()}-W{df['week_of_year'].max()}")

    logger.info("\n[2a/2] Training LightGBM...")
    lgb_result = train_lightgbm(df)
    log_lightgbm_run(lgb_result)

    logger.info(f"\n[2b/2] Training LSTM on {DEVICE}...")
    lstm_result = train_lstm(df)
    log_lstm_run(lstm_result)

    logger.info("\n" + "=" * 60)
    logger.info("COMPARISON")
    logger.info(f"  LightGBM RMSE: {lgb_result['rmse']:,.2f}")
    logger.info(f"  LSTM     RMSE: {lstm_result['rmse']:,.2f}")
    winner = "LightGBM" if lgb_result["rmse"] < lstm_result["rmse"] else "LSTM"
    logger.info(f"  → Winner: {winner}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem chi tiết.")
