import pytest

from src.chatbot.backend import lakehouse_tools


def test_guard_read_only_sql_accepts_select_and_with():
    assert lakehouse_tools.guard_read_only_sql("SELECT * FROM iceberg.gold.fact_order")
    assert lakehouse_tools.guard_read_only_sql(
        "WITH x AS (SELECT 1 AS value) SELECT value FROM x"
    )


@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM iceberg.gold.fact_order",
        "SELECT * FROM iceberg.gold.fact_order; DROP TABLE iceberg.gold.fact_order",
        "UPDATE iceberg.gold.fact_order SET order_status = 'x'",
    ],
)
def test_guard_read_only_sql_rejects_unsafe_sql(sql):
    with pytest.raises(ValueError):
        lakehouse_tools.guard_read_only_sql(sql)


def test_get_table_metadata_falls_back_to_yaml(monkeypatch):
    def fail_trino(_sql):
        raise RuntimeError("trino unavailable")

    monkeypatch.setattr(lakehouse_tools, "_trino_fetchall", fail_trino)

    metadata = lakehouse_tools.get_table_metadata("dim_product")
    column_names = [col["name"] for col in metadata["columns"]]
    product_key = next(col for col in metadata["columns"] if col["name"] == "product_key")

    assert metadata["table_name"] == "iceberg.gold.dim_product"
    assert metadata["metadata_source"] == "yaml_fallback"
    assert "product_weight_g" in column_names
    assert product_key["is_key"] is True
