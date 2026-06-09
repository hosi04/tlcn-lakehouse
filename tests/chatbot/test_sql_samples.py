from pathlib import Path

import yaml


SQL_SAMPLES_PATH = (
    Path(__file__).resolve().parents[2]
    / "src"
    / "chatbot"
    / "backend"
    / "mcp_server"
    / "sql_samples.yaml"
)


def test_sql_samples_include_hard_benchmark_patterns():
    samples = yaml.safe_load(SQL_SAMPLES_PATH.read_text(encoding="utf-8"))["samples"]
    ids = {sample["id"] for sample in samples}

    assert "late_order_rate_by_customer_state_2018" in ids
    assert "fastest_sellers_min_50_items" in ids
    assert "customer_seller_city_gap" in ids
    assert "weekend_top_categories_by_revenue" in ids


def test_sql_samples_do_not_use_known_invalid_columns():
    samples = yaml.safe_load(SQL_SAMPLES_PATH.read_text(encoding="utf-8"))["samples"]
    all_sql = "\n".join(sample["sql"].lower() for sample in samples)

    assert "dp.product_name" not in all_sql
    assert "ds.customer_key" not in all_sql
    assert "d.delivery_actual_days" not in all_sql
    assert "cast(d.day_of_week as varchar)" not in all_sql
