from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any


NL2SQL_THRESHOLDS = {
    "easy": 0.90,
    "medium": 0.75,
    "hard": 0.50,
    "overall": 0.70,
}

@dataclass(frozen=True)
class EvaluationResult:
    execution_success: bool
    table_score: float
    keyword_score: float
    forbidden_score: float
    row_count: int
    passed: bool
    reason: str


def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql or "").strip().lower()


def _sql_mentions_table(sql: str, table: str) -> bool:
    normalized = normalize_sql(sql)
    table = table.lower()
    short_name = table.split(".")[-1]
    patterns = [
        rf"\b{re.escape(table)}\b",
        rf"\b{re.escape(short_name)}\b",
    ]
    return any(re.search(pattern, normalized) for pattern in patterns)


def _score_expected_items(expected: list[str], predicate) -> float:
    if not expected:
        return 1.0
    matched = sum(1 for item in expected if predicate(item))
    return matched / len(expected)


def _score_any_expected_set(expected_sets: list[list[str]], predicate) -> float:
    if not expected_sets:
        return 1.0
    scores = [_score_expected_items(expected, predicate) for expected in expected_sets]
    return max(scores) if scores else 1.0


def evaluate_nl2sql_result(question: dict[str, Any], response: dict[str, Any]) -> EvaluationResult:
    sql = response.get("sql") or ""
    row_count = response.get("row_count", -1)
    min_row_count = question.get("min_row_count", 1)
    execution_success = (
        bool(response.get("success"))
        and bool(sql)
        and row_count >= 0
        and not response.get("error")
    )

    expected_tables = question.get("expected_tables", [])
    expected_keywords = question.get("expected_sql_contains", [])
    expected_table_sets = question.get("expected_any_table_sets", [])
    expected_keyword_sets = question.get("expected_any_sql_contains", [])
    forbidden_keywords = question.get("forbidden_sql_contains", [])
    max_row_count = question.get("max_row_count")

    if expected_tables:
        expected_table_sets = [expected_tables]
    if expected_keywords:
        expected_keyword_sets = [expected_keywords]

    table_score = _score_any_expected_set(
        expected_table_sets,
        lambda table: _sql_mentions_table(sql, table),
    )
    keyword_score = _score_any_expected_set(
        expected_keyword_sets,
        lambda keyword: keyword.lower() in normalize_sql(sql),
    )
    forbidden_hits = [
        keyword
        for keyword in forbidden_keywords
        if keyword.lower() in normalize_sql(sql)
    ]
    forbidden_score = 1.0 if not forbidden_hits else 0.0

    enough_rows = row_count >= min_row_count
    within_max_rows = max_row_count is None or row_count <= max_row_count
    passed = (
        execution_success
        and enough_rows
        and within_max_rows
        and table_score == 1.0
        and keyword_score == 1.0
        and forbidden_score == 1.0
    )
    if not execution_success:
        reason = "execution_failed"
    elif not enough_rows:
        reason = "row_count_below_min"
    elif not within_max_rows:
        reason = "row_count_above_max"
    elif table_score < 1.0:
        reason = "missing_expected_table"
    elif keyword_score < 1.0:
        reason = "missing_expected_sql_keyword"
    elif forbidden_score < 1.0:
        reason = "forbidden_sql_keyword"
    else:
        reason = "passed"

    return EvaluationResult(
        execution_success=execution_success,
        table_score=round(table_score, 3),
        keyword_score=round(keyword_score, 3),
        forbidden_score=forbidden_score,
        row_count=row_count,
        passed=passed,
        reason=reason,
    )


def summarize_thresholds(results: list[dict[str, Any]]) -> tuple[bool, list[dict[str, Any]]]:
    summaries = []
    all_passed = True

    for difficulty in ["easy", "medium", "hard"]:
        subset = [r for r in results if r["difficulty"] == difficulty]
        if not subset:
            continue
        total = len(subset)
        passed = sum(1 for r in subset if r["passed"])
        rate = passed / total
        threshold = NL2SQL_THRESHOLDS[difficulty]
        ok = rate >= threshold
        all_passed = all_passed and ok
        summaries.append({
            "difficulty": difficulty,
            "total": total,
            "passed": passed,
            "pass_rate": rate,
            "threshold": threshold,
            "status": "PASS" if ok else "FAIL",
        })

    if results:
        total = len(results)
        passed = sum(1 for r in results if r["passed"])
        rate = passed / total
        threshold = NL2SQL_THRESHOLDS["overall"]
        ok = rate >= threshold
        all_passed = all_passed and ok
        summaries.append({
            "difficulty": "overall",
            "total": total,
            "passed": passed,
            "pass_rate": rate,
            "threshold": threshold,
            "status": "PASS" if ok else "FAIL",
        })

    return all_passed, summaries
