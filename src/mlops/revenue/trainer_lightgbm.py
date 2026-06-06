from __future__ import annotations

import math
import json
import logging
import tempfile
from pathlib import Path

import numpy  as np
import pandas as pd
import mlflow
import mlflow.lightgbm
import mlflow.sklearn
import lightgbm as lgb
from sklearn.model_selection import ParameterGrid
from sklearn.preprocessing   import StandardScaler
from sklearn.metrics          import mean_squared_error, mean_absolute_error
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup  import setup_mlflow
from src.mlops.data_loader         import load_revenue_data
from src.mlops.revenue.features import (
    TRAIN_RATIO,
    TARGET_COL,
    build_lag_features,
)

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EXPERIMENT = "P1_Revenue_Forecasting"

_LGB_EXCLUDE: set[str] = {
    "year", "week_of_year",
    TARGET_COL,
    "total_product_value", "total_freight_value",
}

LGB_GRID = ParameterGrid({
    "learning_rate":    [0.01, 0.03, 0.05],
    "num_leaves":       [3, 7, 15],
    "min_data_in_leaf": [1, 3, 5],
    "feature_fraction": [0.8, 1.0],
})


def train_lightgbm(
    df: pd.DataFrame,
    learning_rate:    float,
    num_leaves:       int,
    min_data_in_leaf: int,
    feature_fraction: float,
) -> dict:
    df_lag = build_lag_features(df)
    features = [
        c for c in df_lag.columns
        if c not in _LGB_EXCLUDE
        and df_lag[c].dtype in ("float64", "int64", "float32", "int32")
    ]
    split   = int(len(df_lag) * TRAIN_RATIO)
    X_train = df_lag[features][:split]
    X_val   = df_lag[features][split:]
    y_train = df_lag[TARGET_COL][:split]
    y_val   = df_lag[TARGET_COL][split:]

    scaler    = StandardScaler()
    X_train_s = scaler.fit_transform(X_train)
    X_val_s   = scaler.transform(X_val)

    params = {
        "objective":        "regression",
        "metric":           "rmse",
        "learning_rate":    learning_rate,
        "num_leaves":       num_leaves,
        "min_data_in_leaf": min_data_in_leaf,
        "feature_fraction": feature_fraction,
        "bagging_fraction": 0.8,
        "bagging_freq":     1,
        "lambda_l2":        1.0,
        "seed":             42,
        "verbose":          -1,
    }
    lgb_train = lgb.Dataset(X_train_s, np.log1p(y_train))
    lgb_val   = lgb.Dataset(X_val_s, np.log1p(y_val), reference=lgb_train)
    model = lgb.train(
        params, lgb_train, num_boost_round=300, valid_sets=[lgb_val],
        callbacks=[lgb.early_stopping(30, verbose=False), lgb.log_evaluation(50)],
    )
    preds = np.maximum(np.expm1(model.predict(X_val_s)), 0)
    rmse  = math.sqrt(mean_squared_error(y_val, preds))
    mae   = mean_absolute_error(y_val, preds)
    return {"model": model, "scaler": scaler, "rmse": rmse, "mae": mae,
            "features": features}


def run_lightgbm_grid(df: pd.DataFrame) -> tuple[dict, dict, float]:
    best_rmse, best_cfg, best_result, best_run_id = float("inf"), None, None, None

    with mlflow.start_run(run_name="LightGBM_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")
        for i, cfg in enumerate(LGB_GRID):
            run_name = (
                f"lgb_lr{cfg['learning_rate']}"
                f"_nl{cfg['num_leaves']}"
                f"_md{cfg['min_data_in_leaf']}"
            )
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"    [{i+1}/{len(LGB_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",  "lightgbm")
                mlflow.log_param("train_ratio", TRAIN_RATIO)
                result = train_lightgbm(df, **cfg)
                mlflow.log_metric("rmse", result["rmse"])
                mlflow.log_metric("mae",  result["mae"])
                mlflow.lightgbm.log_model(result["model"],  artifact_path="model")
                mlflow.sklearn.log_model(result["scaler"], artifact_path="scaler")
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


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("LightGBM Grid Search — P1 Revenue Forecasting")
    logger.info("=" * 60)

    df = load_revenue_data().sort_values(["year", "week_of_year"]).reset_index(drop=True)
    logger.info(f"  {len(df)} weekly records loaded")

    result, cfg, rmse = run_lightgbm_grid(df)
    logger.info(f"\n  Champion RMSE: {rmse:,.2f} | Config: {cfg}")
    logger.info("  Mở MLflow UI tại http://localhost:5000 để xem Nested Runs.")
