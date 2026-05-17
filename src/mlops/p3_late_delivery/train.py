import logging
import numpy  as np
import pandas as pd
import mlflow
import mlflow.pytorch
import torch
import torch.nn as nn
from torch.utils.data    import DataLoader, TensorDataset
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing   import MinMaxScaler
from sklearn.metrics         import (
    accuracy_score, precision_score, recall_score, f1_score, roc_auc_score,
    classification_report,
)
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup      import setup_mlflow
from src.mlops.data_loader             import load_late_delivery_data
from src.mlops.p3_late_delivery.model  import LateDeliveryLSTM

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

SEQ_LEN     = 5
TRAIN_RATIO = 0.8
EPOCHS      = 60
DEVICE      = torch.device("cuda" if torch.cuda.is_available() else "cpu")
EXPERIMENT  = "P3_Late_Delivery_Prediction"

LABEL_COL = "is_late_week"

SEQUENCE_FEATURE_COLS = [
    "avg_delivery_days",
    "avg_estimate_days",
    "order_count",
    "late_rate",
    LABEL_COL,
]

LSTM_GRID = ParameterGrid({
    "lr":         [1e-3, 5e-4],
    "batch_size": [32, 64],
})


def build_seller_sequences(df: pd.DataFrame, seq_len: int) -> tuple:
    """Build seller windows from past weeks to predict the next week."""
    X_list, y_list, label_time_keys = [], [], []

    for seller_key, group in df.groupby("seller_key"):
        group = group.sort_values(["year", "week_of_year"]).reset_index(drop=True)
        if len(group) < seq_len + 1:
            continue
        for i in range(len(group) - seq_len):
            X_list.append(group[SEQUENCE_FEATURE_COLS].iloc[i: i + seq_len].values)
            y_list.append(int(group[LABEL_COL].iloc[i + seq_len]))
            label_row = group.iloc[i + seq_len]
            label_time_keys.append(int(label_row["year"]) * 100 + int(label_row["week_of_year"]))

    X = np.array(X_list, dtype=np.float32)
    y = np.array(y_list, dtype=np.float32).reshape(-1, 1)
    label_time_keys = np.array(label_time_keys)
    logger.info(f"[sequences] {len(X)} samples | late_rate={y.mean()*100:.1f}%")
    return X, y, label_time_keys


def train_model(X_tr, y_tr, X_val, y_val,
                lr: float, batch_size: int, pos_weight: float) -> LateDeliveryLSTM:
    ds_train = TensorDataset(torch.tensor(X_tr), torch.tensor(y_tr))
    ds_val   = TensorDataset(torch.tensor(X_val), torch.tensor(y_val))
    dl_train = DataLoader(ds_train, batch_size=batch_size, shuffle=True)
    dl_val   = DataLoader(ds_val,   batch_size=batch_size)

    model     = LateDeliveryLSTM(input_size=len(SEQUENCE_FEATURE_COLS)).to(DEVICE)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    pos_weight_tensor = torch.tensor([pos_weight], dtype=torch.float32).to(DEVICE)
    criterion = nn.BCEWithLogitsLoss(pos_weight=pos_weight_tensor)
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
            logger.info(f"    Epoch {epoch:3d}/{EPOCHS} | val_loss={val_loss:.4f}")

    model.load_state_dict(best_state)
    return model


def find_best_threshold(truth, probs, metric: str = "f1") -> tuple:
    thresholds = np.arange(0.1, 0.91, 0.01)
    best_threshold, best_score = 0.5, -1.0
    for threshold in thresholds:
        preds = (probs >= threshold).astype(int)
        score = (
            accuracy_score(truth, preds)
            if metric == "accuracy"
            else f1_score(truth, preds, zero_division=0)
        )
        if score > best_score:
            best_threshold, best_score = float(threshold), float(score)
    return best_threshold, best_score


def evaluate(model: LateDeliveryLSTM, X_val, y_val) -> dict:
    model.eval()
    with torch.no_grad():
        logits = model(torch.tensor(X_val).to(DEVICE))
        probs  = torch.sigmoid(logits).cpu().numpy().flatten()
    truth = y_val.flatten().astype(int)
    best_f1_threshold, _ = find_best_threshold(truth, probs, metric="f1")
    best_acc_threshold, _ = find_best_threshold(truth, probs, metric="accuracy")
    preds = (probs >= best_f1_threshold).astype(int)
    preds_05 = (probs >= 0.5).astype(int)
    report = classification_report(truth, preds, target_names=["Đúng hạn", "Giao trễ"])
    logger.info(f"\nClassification Report:\n{report}")
    return {
        "threshold": best_f1_threshold,
        "accuracy_at_0_5": accuracy_score(truth, preds_05),
        "f1_at_0_5": f1_score(truth, preds_05, zero_division=0),
        "best_accuracy_threshold": best_acc_threshold,
        "accuracy":  accuracy_score(truth, preds),
        "precision": precision_score(truth, preds, zero_division=0),
        "recall":    recall_score(truth, preds,    zero_division=0),
        "f1":        f1_score(truth, preds,         zero_division=0),
        "roc_auc":   roc_auc_score(truth, probs) if len(np.unique(truth)) > 1 else 0.0,
    }


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("P3 — Late Delivery Prediction (LSTM Grid Search)")
    logger.info("=" * 60)

    logger.info("\n[1/3] Loading data...")
    df = load_late_delivery_data(min_weeks=SEQ_LEN + 1)

    logger.info(f"\n[2/3] Building sequences (seq_len={SEQ_LEN})...")
    X, y, label_time_keys = build_seller_sequences(df, SEQ_LEN)

    unique_time_keys = np.sort(np.unique(label_time_keys))
    cutoff_idx = max(1, int(len(unique_time_keys) * TRAIN_RATIO))
    cutoff_key = unique_time_keys[min(cutoff_idx, len(unique_time_keys) - 1)]
    train_mask = label_time_keys < cutoff_key
    val_mask   = label_time_keys >= cutoff_key
    X_tr, X_val = X[train_mask], X[val_mask]
    y_tr, y_val = y[train_mask], y[val_mask]

    n_train, seq_len_dim, n_features = X_tr.shape
    scaler     = MinMaxScaler()
    X_tr_flat  = scaler.fit_transform(X_tr.reshape(-1, n_features))
    X_val_flat = scaler.transform(X_val.reshape(-1, n_features))
    X_tr       = X_tr_flat.reshape(n_train, seq_len_dim, n_features).astype(np.float32)
    X_val      = X_val_flat.reshape(len(X_val), seq_len_dim, n_features).astype(np.float32)

    pos_count  = y_tr.sum()
    neg_count  = len(y_tr) - pos_count
    pos_weight = float(neg_count / max(pos_count, 1))
    logger.info(f"  Train: {len(X_tr)} | Val: {len(X_val)}")
    logger.info(f"  Late ratio: {y.mean()*100:.1f}% | pos_weight: {pos_weight:.2f}")

    logger.info(f"\n[3/3] LSTM Grid Search ({len(LSTM_GRID)} configs) on {DEVICE}...")

    best_f1, best_cfg, best_metrics = 0.0, None, None
    best_run_id = None

    with mlflow.start_run(run_name="LSTM_Classifier_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")

        for i, cfg in enumerate(LSTM_GRID):
            run_name = f"lstm_lr{cfg['lr']}_bs{cfg['batch_size']}"
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"\n  [{i+1}/{len(LSTM_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",    "lstm_classifier_pytorch")
                mlflow.log_param("seq_len",       SEQ_LEN)
                mlflow.log_param("epochs",        EPOCHS)
                mlflow.log_param("train_ratio",   TRAIN_RATIO)
                mlflow.log_param("n_samples",     len(X))
                mlflow.log_param("late_rate_pct", round(float(y.mean() * 100), 2))
                mlflow.log_param("pos_weight",    round(pos_weight, 4))

                model   = train_model(X_tr, y_tr, X_val, y_val, **cfg, pos_weight=pos_weight)
                mlflow.log_param("hidden_size",   model.hidden_size)
                mlflow.log_param("num_layers",    model.num_layers)
                metrics = evaluate(model, X_val, y_val)

                for k, v in metrics.items():
                    mlflow.log_metric(k, round(v, 4))
                mlflow.pytorch.log_model(model, artifact_path="model")

                logger.info(
                    f"    → F1={metrics['f1']:.4f} | "
                    f"Acc={metrics['accuracy']:.4f} | "
                    f"ROC-AUC={metrics['roc_auc']:.4f} | "
                    f"Threshold={metrics['threshold']:.2f}"
                )

                if metrics["f1"] > best_f1:
                    best_f1      = metrics["f1"]
                    best_cfg     = cfg
                    best_metrics = metrics
                    best_run_id  = child.info.run_id

        mlflow.log_params({f"best_{k}": v for k, v in best_cfg.items()})
        mlflow.log_metric("best_f1", best_f1)
        logger.info(f"\n  ✅ Best: F1={best_f1:.4f} | Config={best_cfg}")

    mlflow.register_model(f"runs:/{best_run_id}/model", "late_delivery_lstm")
    logger.info("  📦 Registered model: late_delivery_lstm")

    logger.info("\n" + "=" * 60)
    logger.info("GRID SEARCH SUMMARY")
    logger.info(f"  Best Config  : {best_cfg}")
    logger.info(f"  Accuracy     : {best_metrics['accuracy']:.4f}")
    logger.info(f"  Precision    : {best_metrics['precision']:.4f}")
    logger.info(f"  Recall       : {best_metrics['recall']:.4f}")
    logger.info(f"  F1           : {best_metrics['f1']:.4f}")
    logger.info(f"  ROC-AUC      : {best_metrics['roc_auc']:.4f}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem Nested Runs.")
