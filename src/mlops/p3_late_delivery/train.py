import logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from torch.utils.data    import DataLoader, TensorDataset
from sklearn.preprocessing import MinMaxScaler
from sklearn.metrics        import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
    classification_report,
)
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup       import setup_mlflow
from src.mlops.data_loader              import load_late_delivery_data
from src.mlops.p3_late_delivery.model  import LateDeliveryLSTM

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SEQ_LEN    = 5
TRAIN_RATIO= 0.8
EPOCHS     = 60
LR         = 1e-3
BATCH_SIZE = 64
DEVICE     = torch.device("cuda" if torch.cuda.is_available() else "cpu")
EXPERIMENT = "P3_Late_Delivery_Prediction"

FEATURE_COLS = [
    "avg_delivery_days",
    "avg_estimate_days",
    "order_count",
    "late_rate",
    "is_late_week",
]
LABEL_COL = "is_late_week"

def build_seller_sequences(df: pd.DataFrame, seq_len: int) -> tuple:
    scaler = MinMaxScaler()
    df_scaled = df.copy()
    df_scaled[FEATURE_COLS] = scaler.fit_transform(df_scaled[FEATURE_COLS])

    X_list, y_list = [], []
    for seller_key, group in df_scaled.groupby("seller_key"):
        group = group.sort_values(["year", "week_of_year"]).reset_index(drop=True)
        if len(group) < seq_len + 1:
            continue
        for i in range(len(group) - seq_len):
            X_list.append(group[FEATURE_COLS].iloc[i : i + seq_len].values)
            y_list.append(int(group[LABEL_COL].iloc[i + seq_len]))

    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.float32).reshape(-1, 1)
    logger.info(f"[sequences] {len(X)} samples | late_rate={y.mean()*100:.1f}%")
    return X, y, scaler


def train_model(X_tr, y_tr, X_val, y_val, pos_weight: float) -> LateDeliveryLSTM:
    ds_train = TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr))
    ds_val   = TensorDataset(torch.tensor(X_val), torch.tensor(y_val))
    dl_train = DataLoader(ds_train, batch_size=BATCH_SIZE, shuffle=True)
    dl_val   = DataLoader(ds_val,   batch_size=BATCH_SIZE)

    model     = LateDeliveryLSTM(input_size=len(FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=LR)
    criterion = nn.BCELoss()
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=8, factor=0.5)

    best_val_loss = float("inf")
    best_state    = None

    for epoch in range(1, EPOCHS + 1):
        model.train()
        for xb, yb in dl_train:
            xb, yb = xb.to(DEVICE), yb.to(DEVICE)
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
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

        if epoch % 10 == 0 or epoch == 1:
            logger.info(f"  Epoch {epoch:3d}/{EPOCHS} | val_loss={val_loss:.4f}")

    model.load_state_dict(best_state)
    return model

def evaluate(model: LateDeliveryLSTM, X_val, y_val) -> dict:
    model.eval()
    with torch.no_grad():
        probs = model(torch.tensor(X_val).to(DEVICE)).cpu().numpy().flatten()
    preds = (probs >= 0.5).astype(int)
    truth = y_val.flatten().astype(int)

    report = classification_report(truth, preds, target_names=["Đúng hạn", "Giao trễ"])
    logger.info(f"\nClassification Report:\n{report}")

    return {
        "accuracy":  accuracy_score(truth, preds),
        "precision": precision_score(truth, preds, zero_division=0),
        "recall":    recall_score(truth, preds,    zero_division=0),
        "f1":        f1_score(truth, preds,         zero_division=0),
        "roc_auc":   roc_auc_score(truth, probs) if len(np.unique(truth)) > 1 else 0.0,
    }

if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("P3 — Late Delivery Prediction (LSTM Classifier)")
    logger.info("=" * 60)

    logger.info("\n[1/3] Loading data...")
    df = load_late_delivery_data(min_weeks=SEQ_LEN + 1)

    logger.info(f"\n[2/3] Building sequences (seq_len={SEQ_LEN})...")
    X, y, scaler = build_seller_sequences(df, SEQ_LEN)

    split   = int(len(X) * TRAIN_RATIO)
    X_tr, X_val = X[:split], X[split:]
    y_tr, y_val = y[:split], y[split:]

    pos_count = y_tr.sum()
    neg_count = len(y_tr) - pos_count
    pos_weight = neg_count / max(pos_count, 1)
    logger.info(f"  Train samples: {len(X_tr)} | Val: {len(X_val)}")
    logger.info(f"  Late ratio: {y.mean()*100:.1f}% | pos_weight: {pos_weight:.2f}")

    logger.info(f"\n[3/3] Training LSTM Classifier on {DEVICE}...")
    with mlflow.start_run(run_name="LSTM_Classifier"):
        mlflow.log_param("model_type",   "lstm_classifier_pytorch")
        mlflow.log_param("seq_len",      SEQ_LEN)
        mlflow.log_param("hidden_size",  32)
        mlflow.log_param("num_layers",   1)
        mlflow.log_param("epochs",       EPOCHS)
        mlflow.log_param("lr",           LR)
        mlflow.log_param("batch_size",   BATCH_SIZE)
        mlflow.log_param("train_ratio",  TRAIN_RATIO)
        mlflow.log_param("n_samples",    len(X))
        mlflow.log_param("late_rate_pct", round(float(y.mean() * 100), 2))

        model = train_model(X_tr, y_tr, X_val, y_val, pos_weight)
        metrics = evaluate(model, X_val, y_val)

        for k, v in metrics.items():
            mlflow.log_metric(k, round(v, 4))

        mlflow.pytorch.log_model(
            model,
            artifact_path="model",
            registered_model_name="late_delivery_lstm",
        )

        logger.info("\n✅ Training complete!")
        logger.info(f"   Accuracy : {metrics['accuracy']:.4f}")
        logger.info(f"   Precision: {metrics['precision']:.4f}")
        logger.info(f"   Recall   : {metrics['recall']:.4f}")
        logger.info(f"   F1       : {metrics['f1']:.4f}")
        logger.info(f"   ROC-AUC  : {metrics['roc_auc']:.4f}")
        logger.info("   Mở MLflow UI tại http://localhost:5000 để xem.")
