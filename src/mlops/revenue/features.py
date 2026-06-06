from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

TRAIN_RATIO = 0.8
SEED        = 42
TARGET_COL  = "weekly_revenue"

LSTM_EXCLUDE_COLS: set[str] = {
    "year",
    "week_of_year",
    "total_product_value",
    "total_freight_value",
}


def add_calendar_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["week_sin"]  = np.sin(2 * np.pi * df["week_of_year"] / 52)
    df["week_cos"]  = np.cos(2 * np.pi * df["week_of_year"] / 52)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    return df


def build_lag_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["year", "week_of_year"]).copy()
    df = add_calendar_features(df)
    lag_cols = [c for c in df.columns if "lag_" in c or "rolling_" in c]
    if lag_cols:
        df = df.dropna(subset=lag_cols)
    return df.reset_index(drop=True)


def get_lstm_feature_cols(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if c not in LSTM_EXCLUDE_COLS
        and df[c].dtype in ("float64", "int64", "float32", "int32")
    ]


def build_sequences(
    df: pd.DataFrame,
    seq_len: int,
    feature_cols: list[str] | None = None,
) -> tuple[np.ndarray, np.ndarray, StandardScaler, StandardScaler, list[str]]:
    df = build_lag_features(df)
    feature_cols = feature_cols or get_lstm_feature_cols(df)

    train_end = int((len(df) - seq_len) * TRAIN_RATIO) + seq_len

    feat_scaler = StandardScaler()
    tgt_scaler  = StandardScaler()
    target_log  = np.log1p(df[[TARGET_COL]].values)

    feat_scaler.fit(df[feature_cols].iloc[:train_end])
    tgt_scaler.fit(target_log[:train_end])

    X_raw = feat_scaler.transform(df[feature_cols])
    y_raw = tgt_scaler.transform(target_log)

    X_seq, y_seq = [], []
    for i in range(len(df) - seq_len):
        X_seq.append(X_raw[i: i + seq_len])
        y_seq.append(y_raw[i + seq_len])

    return (
        np.array(X_seq, dtype=np.float32),
        np.array(y_seq, dtype=np.float32),
        feat_scaler,
        tgt_scaler,
        feature_cols,
    )
