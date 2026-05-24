import logging
from pathlib import Path
from typing import Any, Dict

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_CONFIG_PATH = _PROJECT_ROOT / "config" / "silver_rules.yaml"


def load_yaml_config(path: str | None = None) -> Dict[str, Any]:
    """
    Load silver_rules.yaml.

    Args:
        path: Optional path override. Defaults to config/silver_rules.yaml.

    Returns:
        Parsed YAML dict.

    Raises:
        FileNotFoundError: If the config file does not exist.
    """
    config_path = Path(path) if path else _DEFAULT_CONFIG_PATH
    if not config_path.exists():
        raise FileNotFoundError(
            f"Silver rules config not found: {config_path}\n"
            f"Expected location: {_DEFAULT_CONFIG_PATH}"
        )
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def apply_select(df: DataFrame, rule: Dict) -> DataFrame:
    cols = rule.get("select")
    if not cols:
        return df
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"[{rule.get('target', '?')}] select: columns not found: {missing}"
        )
    return df.select(*cols)


def apply_drop_columns(df: DataFrame, rule: Dict) -> DataFrame:
    cols = rule.get("drop_columns") or []
    existing = [c for c in cols if c in df.columns]
    skipped = set(cols) - set(existing)
    if skipped:
        logger.warning(
            "[%s] drop_columns: columns not found, skipped: %s",
            rule.get("target", "?"), skipped
        )
    return df.drop(*existing) if existing else df


def apply_casts(df: DataFrame, rule: Dict) -> DataFrame:
    casts = rule.get("casts") or {}
    for col_name, target_type in casts.items():
        if col_name not in df.columns:
            logger.warning(
                "[%s] casts: column '%s' not found, skipped.",
                rule.get("target", "?"), col_name
            )
            continue
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def apply_rounds(df: DataFrame, rule: Dict) -> DataFrame:
    rounds = rule.get("round") or {}
    for col_name, opts in rounds.items():
        if col_name not in df.columns:
            logger.warning(
                "[%s] round: column '%s' not found, skipped.",
                rule.get("target", "?"), col_name
            )
            continue
        scale = opts.get("scale", 2)
        target_type = opts.get("type", "double")
        df = df.withColumn(
            col_name,
            F.round(F.col(col_name), scale).cast(target_type)
        )
    return df


def apply_drop_nulls(df: DataFrame, rule: Dict) -> DataFrame:
    setting = rule.get("drop_nulls")
    if setting is True:
        return df.na.drop()
    if isinstance(setting, list) and setting:
        return df.na.drop(subset=setting)
    return df


def apply_drop_duplicates(df: DataFrame, rule: Dict) -> DataFrame:
    setting = rule.get("drop_duplicates")
    if setting is True:
        return df.dropDuplicates()
    if isinstance(setting, dict):
        subset = setting.get("subset")
        if subset:
            return df.dropDuplicates(subset)
    return df

def _geolocation_brazil_zip_agg(df: DataFrame) -> DataFrame:
    df = df.na.drop()
    df = df.filter(
        (F.col("geolocation_lat") <= 5.27438888)
        & (F.col("geolocation_lng") >= -73.98283055)
        & (F.col("geolocation_lat") >= -33.75116944)
        & (F.col("geolocation_lng") <= -34.79314722)
    )
    df = df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("geolocation_lat"),
        F.avg("geolocation_lng").alias("geolocation_lng"),
        F.first("geolocation_city").alias("geolocation_city"),
        F.first("geolocation_state").alias("geolocation_state"),
    )
    return df


def _date_from_orders(df: DataFrame) -> DataFrame:
    return (
        df.select("order_purchase_timestamp")
        .na.drop()
        .dropDuplicates()
    )


_CUSTOM_TRANSFORMS = {
    "geolocation_brazil_zip_agg": _geolocation_brazil_zip_agg,
    "date_from_orders": _date_from_orders,
}


def apply_custom_transform(df: DataFrame, rule: Dict) -> DataFrame:
    name = rule.get("custom_transform")
    fn = _CUSTOM_TRANSFORMS.get(name)
    if fn is None:
        raise ValueError(
            f"Unknown custom_transform: '{name}'. "
            f"Registered: {list(_CUSTOM_TRANSFORMS.keys())}"
        )
    logger.debug("Applying custom_transform: %s", name)
    return fn(df)


def apply_rules(df: DataFrame, rule: Dict) -> DataFrame:
    if rule.get("custom_transform"):
        return apply_custom_transform(df, rule)

    df = apply_select(df, rule)
    df = apply_drop_columns(df, rule)
    df = apply_casts(df, rule)
    df = apply_rounds(df, rule)
    df = apply_drop_nulls(df, rule)
    df = apply_drop_duplicates(df, rule)
    return df


def summarise_rules(rule: Dict) -> list:
    if rule.get("custom_transform"):
        return [f"custom_transform:{rule['custom_transform']}"]
    active = []
    for key in ("select", "drop_columns", "casts", "round", "drop_nulls", "drop_duplicates"):
        if rule.get(key):
            active.append(key)
    return active
