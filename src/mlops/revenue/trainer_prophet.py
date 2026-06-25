from __future__ import annotations

import math
import logging

import numpy  as np
import pandas as pd
import mlflow
import mlflow.prophet
from sklearn.model_selection import ParameterGrid
from sklearn.metrics          import mean_squared_error, mean_absolute_error
from dotenv import load_dotenv

from src.mlops.utils.mlflow_setup  import setup_mlflow
from src.mlops.data_loader         import load_revenue_data
from src.mlops.revenue.features    import TRAIN_RATIO, TARGET_COL, build_lag_features

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

EXPERIMENT = "P1_Revenue_Forecasting"

PROPHET_REGRESSORS = [
    "order_count",
    "active_customers_count",
    "avg_order_value",
    "avg_items_per_order",
    "freight_revenue_ratio",
    "avg_freight_per_item",
    "avg_delivery_days",
    "avg_estimate_days",
    "late_delivery_rate",
    "credit_card_ratio",
    "avg_installments",
    "weekend_order_ratio",
    "active_sellers_count",
    "revenue_lag_1",
    "revenue_lag_2",
    "revenue_lag_4",
    "rolling_avg_4w",
    "rolling_std_4w",
    "rolling_avg_8w",
    "order_count_lag_1",
    "order_count_lag_2",
    "order_count_lag_4",
    "order_rolling_avg_4w",
    "revenue_momentum",
    "revenue_acceleration",
]

REQUIRED_COLUMNS = {"year", "week_of_year", TARGET_COL}
MIN_HISTORY_WEEKS = 8

PROPHET_GRID = ParameterGrid({
    "changepoint_prior_scale":  [0.01, 0.05, 0.1, 0.3],
    "seasonality_prior_scale":  [0.1,  1.0,  5.0],
    "seasonality_mode":         ["additive", "multiplicative"],
})


def _build_prophet_df(df: pd.DataFrame) -> pd.DataFrame:
    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required Prophet columns: {sorted(missing_cols)}")

    df = build_lag_features(df)
    if len(df) <= MIN_HISTORY_WEEKS:
        raise ValueError(
            f"Not enough rows for Prophet after feature building: {len(df)} rows"
        )
    df = df.iloc[MIN_HISTORY_WEEKS:].copy()

    df["ds"] = df.apply(
        lambda r: pd.Timestamp.fromisocalendar(int(r["year"]), int(r["week_of_year"]), 1),
        axis=1,
    )
    df["y"] = df[TARGET_COL].values

    keep_cols = ["ds", "y"] + [c for c in PROPHET_REGRESSORS if c in df.columns]
    prophet_df = df[keep_cols].replace([np.inf, -np.inf], np.nan)
    prophet_df = prophet_df.dropna(subset=keep_cols).reset_index(drop=True)
    if prophet_df.empty:
        raise ValueError("Prophet dataframe is empty after dropping invalid rows.")
    return prophet_df


def train_prophet(
    df_prophet: pd.DataFrame,
    changepoint_prior_scale: float,
    seasonality_prior_scale: float,
    seasonality_mode:        str,
) -> dict:
    try:
        from prophet import Prophet
    except ImportError:
        raise ImportError("Chưa cài Prophet. Chạy: pip install prophet")

    split = int(len(df_prophet) * TRAIN_RATIO)
    train = df_prophet.iloc[:split].copy()
    val   = df_prophet.iloc[split:].copy()
    if train.empty or val.empty:
        raise ValueError(
            f"Invalid Prophet split: train={len(train)} rows, val={len(val)} rows"
        )

    regressors_available = [c for c in PROPHET_REGRESSORS if c in df_prophet.columns]

    model = Prophet(
        changepoint_prior_scale = changepoint_prior_scale,
        seasonality_prior_scale = seasonality_prior_scale,
        seasonality_mode        = seasonality_mode,
        yearly_seasonality      = True,
        weekly_seasonality      = False,
        daily_seasonality       = False,
        interval_width          = 0.95,
    )

    for reg in regressors_available:
        model.add_regressor(reg)

    model.fit(train)

    forecast = model.predict(val[["ds"] + regressors_available])
    preds    = np.maximum(forecast["yhat"].values, 0)
    truth    = val["y"].values

    rmse = math.sqrt(mean_squared_error(truth, preds))
    mae  = mean_absolute_error(truth, preds)

    return {
        "model":      model,
        "rmse":       rmse,
        "mae":        mae,
        "preds":      preds,
        "truth":      truth,
        "regressors": regressors_available,
        "val_df":     val,
        "forecast":   forecast,
    }


def run_prophet_grid(df: pd.DataFrame) -> tuple[dict, dict, float]:
    df_prophet = _build_prophet_df(df)
    best_rmse, best_cfg, best_result, best_run_id = float("inf"), None, None, None

    with mlflow.start_run(run_name="Prophet_GridSearch") as parent:
        logger.info(f"  Parent run: {parent.info.run_id}")
        for i, cfg in enumerate(PROPHET_GRID):
            run_name = (
                f"prophet_cp{cfg['changepoint_prior_scale']}"
                f"_sp{cfg['seasonality_prior_scale']}"
                f"_{cfg['seasonality_mode'][:3]}"
            )
            with mlflow.start_run(run_name=run_name, nested=True) as child:
                logger.info(f"    [{i+1}/{len(PROPHET_GRID)}] Config: {cfg}")
                mlflow.log_params(cfg)
                mlflow.log_param("model_type",          "prophet")
                mlflow.log_param("train_ratio",         TRAIN_RATIO)
                mlflow.log_param("regressors",          ", ".join(PROPHET_REGRESSORS))

                result = train_prophet(df_prophet, **cfg)

                mlflow.log_metric("rmse", result["rmse"])
                mlflow.log_metric("mae",  result["mae"])
                mlflow.log_param("regressors_used", len(result["regressors"]))
                mlflow.log_param("regressors_used_names", ", ".join(result["regressors"]))

                mlflow.prophet.log_model(result["model"], artifact_path="model")

                logger.info(f"      → RMSE={result['rmse']:,.2f}  MAE={result['mae']:,.2f}")

                if result["rmse"] < best_rmse:
                    best_rmse, best_cfg, best_result = result["rmse"], cfg, result
                    best_run_id = child.info.run_id

        if best_cfg is None or best_result is None or best_run_id is None:
            raise RuntimeError("Prophet grid search did not complete any run.")

        mlflow.log_params({f"best_{k}": v for k, v in best_cfg.items()})
        mlflow.log_metric("best_rmse", best_rmse)
        logger.info(f"\n  ✅ Prophet Best: RMSE={best_rmse:,.2f} | Config={best_cfg}")

    mlflow.register_model(f"runs:/{best_run_id}/model", "revenue_prophet")
    logger.info("  📦 Registered model: revenue_prophet")
    return best_result, best_cfg, best_rmse


def run_ensemble(
    prophet_result: dict,
    lgb_preds:      np.ndarray,
    lgb_truth:      np.ndarray,
    lgb_dates:      np.ndarray | pd.Series | None = None,
    prophet_weight: float = 0.4,
) -> dict:
    lgb_weight = 1.0 - prophet_weight
    prophet_preds = prophet_result["preds"]

    if lgb_dates is not None and "val_df" in prophet_result:
        lgb_df = pd.DataFrame({
            "ds": pd.to_datetime(lgb_dates),
            "lgb_pred": lgb_preds,
            "truth": lgb_truth,
        })
        prophet_df = pd.DataFrame({
            "ds": pd.to_datetime(prophet_result["val_df"]["ds"].values),
            "prophet_pred": prophet_preds,
        })
        aligned = lgb_df.merge(prophet_df, on="ds", how="inner").sort_values("ds")
        if aligned.empty:
            raise ValueError("No overlapping dates between LightGBM and Prophet predictions.")

        ensemble_preds = (
            prophet_weight * aligned["prophet_pred"].to_numpy()
            + lgb_weight   * aligned["lgb_pred"].to_numpy()
        )
        truth = aligned["truth"].to_numpy()
        dates = aligned["ds"].to_numpy()
    else:
        min_len = min(len(prophet_preds), len(lgb_preds), len(lgb_truth))
        ensemble_preds = (
            prophet_weight * prophet_preds[:min_len]
            + lgb_weight   * lgb_preds[:min_len]
        )
        truth = lgb_truth[:min_len]
        dates = None

    rmse = math.sqrt(mean_squared_error(truth, ensemble_preds))
    mae  = mean_absolute_error(truth, ensemble_preds)
    return {
        "preds":          ensemble_preds,
        "truth":          truth,
        "dates":          dates,
        "rmse":           rmse,
        "mae":            mae,
        "prophet_weight": prophet_weight,
        "lgb_weight":     lgb_weight,
    }


if __name__ == "__main__":
    setup_mlflow(EXPERIMENT)
    logger.info("=" * 60)
    logger.info("Prophet Grid Search — P1 Revenue Forecasting")
    logger.info("=" * 60)

    logger.info("\n[1/2] Loading data từ Iceberg Gold...")
    df = load_revenue_data().sort_values(["year", "week_of_year"]).reset_index(drop=True)
    logger.info(f"      {len(df)} weekly records loaded")

    logger.info(f"\n[2/2] Prophet Grid Search ({len(PROPHET_GRID)} configs)...")
    result, cfg, rmse = run_prophet_grid(df)

    logger.info("\n" + "=" * 60)
    logger.info(f"  Prophet Champion RMSE : {rmse:,.2f}")
    logger.info(f"  Best Config           : {cfg}")
    logger.info("=" * 60)
    logger.info("  MLflow UI: http://localhost:5000")
