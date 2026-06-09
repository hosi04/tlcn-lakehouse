from experiments.etl_benchmark import DATA_QUALITY_CHECKS, TABLES


def test_etl_benchmark_has_gold_tables_and_quality_checks():
    table_names = {full_table for _layer, full_table in TABLES}
    check_names = {check["name"] for check in DATA_QUALITY_CHECKS}

    assert "iceberg.gold.fact_order" in table_names
    assert "iceberg.gold.fact_order_item" in table_names
    assert "fact_order_customer_fk" in check_names
    assert "fact_order_vs_item_product_total" in check_names


def test_quality_checks_are_expected_zero_failure_queries():
    assert DATA_QUALITY_CHECKS
    for check in DATA_QUALITY_CHECKS:
        assert check["expected"] == 0
        assert check["sql"].strip().lower().startswith("select")
