from experiments.evaluation import evaluate_nl2sql_result, summarize_thresholds


def test_nl2sql_evaluation_requires_expected_tables_and_keywords():
    question = {
        "expected_tables": ["iceberg.gold.fact_order"],
        "expected_sql_contains": ["sum", "total_payment_value"],
    }
    response = {
        "success": True,
        "sql": "SELECT SUM(total_payment_value) FROM iceberg.gold.fact_order",
        "row_count": 1,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.execution_success is True
    assert result.passed is True
    assert result.reason == "passed"


def test_nl2sql_evaluation_fails_sql_that_runs_but_uses_wrong_table():
    question = {
        "expected_tables": ["iceberg.gold.fact_order"],
        "expected_sql_contains": ["sum"],
    }
    response = {
        "success": True,
        "sql": "SELECT SUM(item_price) FROM iceberg.gold.fact_order_item",
        "row_count": 1,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.execution_success is True
    assert result.passed is False
    assert result.reason == "missing_expected_table"


def test_nl2sql_threshold_summary_marks_low_hard_score_as_fail():
    results = [
        {"difficulty": "easy", "passed": True},
        {"difficulty": "easy", "passed": True},
        {"difficulty": "hard", "passed": False},
        {"difficulty": "hard", "passed": False},
    ]

    all_ok, rows = summarize_thresholds(results)

    assert all_ok is False
    assert {row["difficulty"]: row["status"] for row in rows}["hard"] == "FAIL"


def test_nl2sql_evaluation_accepts_any_valid_table_and_keyword_set():
    question = {
        "expected_any_table_sets": [
            ["iceberg.gold.fact_order", "iceberg.gold.dim_customer"],
            ["iceberg.gold.fact_order_item", "iceberg.gold.dim_customer"],
        ],
        "expected_any_sql_contains": [
            ["customer_state", "avg", "total_freight_value"],
            ["customer_state", "avg", "item_freight_value"],
        ],
    }
    response = {
        "success": True,
        "sql": """
            SELECT dc.customer_state, AVG(foi.item_freight_value)
            FROM iceberg.gold.fact_order_item foi
            JOIN iceberg.gold.dim_customer dc ON foi.customer_key = dc.customer_key
            GROUP BY dc.customer_state
        """,
        "row_count": 27,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.passed is True


def test_nl2sql_evaluation_fails_empty_result_by_default():
    question = {
        "expected_tables": ["iceberg.gold.dim_product"],
        "expected_sql_contains": ["product_weight_g"],
    }
    response = {
        "success": True,
        "sql": "SELECT * FROM iceberg.gold.dim_product WHERE product_weight_g < 0",
        "row_count": 0,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.passed is False
    assert result.reason == "row_count_below_min"


def test_nl2sql_evaluation_rejects_forbidden_sql_keywords():
    question = {
        "expected_tables": ["iceberg.gold.fact_order"],
        "expected_sql_contains": ["order_status", "count", "group by"],
        "forbidden_sql_contains": ["where order_status = 'delivered'"],
    }
    response = {
        "success": True,
        "sql": """
            SELECT order_status, COUNT(*)
            FROM iceberg.gold.fact_order
            WHERE order_status = 'delivered'
            GROUP BY order_status
        """,
        "row_count": 1,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.passed is False
    assert result.reason == "forbidden_sql_keyword"


def test_nl2sql_evaluation_rejects_too_many_rows_for_scalar_question():
    question = {
        "expected_tables": ["iceberg.gold.fact_order"],
        "expected_sql_contains": ["avg", "total_payment_value"],
        "max_row_count": 1,
    }
    response = {
        "success": True,
        "sql": """
            SELECT order_key, AVG(total_payment_value)
            FROM iceberg.gold.fact_order
            GROUP BY order_key
        """,
        "row_count": 1000,
    }

    result = evaluate_nl2sql_result(question, response)

    assert result.passed is False
    assert result.reason == "row_count_above_max"
