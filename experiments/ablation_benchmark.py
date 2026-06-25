from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from unittest.mock import patch

from experiments.evaluation import evaluate_nl2sql_result
from experiments.nl2sql_benchmark import load_questions
from src.chatbot.backend.agent.graph import run_agent, get_graph
from src.chatbot.backend.retrieval.schema_indexer import ensure_index_ready
from src.chatbot.backend.retrieval.reranker import warmup_reranker
from src.chatbot.backend.llm_connector import get_llm

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

RESULTS_DIR = Path("experiments/results")

CONFIGS = [
    {"name": "BASELINE", "reranker": False, "pruner": False},
    {"name": "RAG_RERANKER", "reranker": True, "pruner": False},
    {"name": "FULL_SYSTEM", "reranker": True, "pruner": True},
]


def passthrough_reranker(question: str, candidates: list[dict], top_k: int = 4) -> list[dict]:
    for c in candidates:
        c["rerank_score"] = 0.0
    return candidates[:top_k]


def passthrough_column_pruner(state: dict) -> dict:
    from src.chatbot.backend.agent.nodes.column_pruner import _format_full_schema
    full_schemas = state.get("full_schemas", {})
    if not full_schemas:
        return {
            "pruned_schema": "",
            "columns_pruned_count": 0,
            "execution_log": state.get("execution_log", []) + ["[column_pruner] Không có schema để prune"],
        }
    full_schema_text = _format_full_schema(full_schemas)
    return {
        "pruned_schema": full_schema_text,
        "columns_pruned_count": 0,
        "schemas_used": list(full_schemas.keys()),
        "execution_log": state.get("execution_log", []) + ["[column_pruner] Passthrough (disabled)"],
    }


def run_config_questions(config_name: str, questions: list[dict]) -> list[dict]:
    rows = []
    logger.info("\n--- %s ---", config_name)
    for i, q in enumerate(questions, 1):
        q_id = q["id"]
        diff = q["difficulty"]
        query = q["query"]

        t0 = time.perf_counter()
        try:
            state = run_agent(query)
            elapsed = time.perf_counter() - t0
            sql = state.get("sql", "")
            row_count = state.get("row_count", 0)
            schemas_used = state.get("schemas_used", [])
            columns_pruned = state.get("columns_pruned_count", 0)
            exec_log = state.get("execution_log", [])

            had_error = any(
                kw in str(log)
                for log in exec_log
                for kw in ("Lỗi", "fix", "Fix", "retry", "sửa", "FAILED", "attempt=2")
            )

            response_data = {
                "success": True,
                "sql": sql,
                "row_count": row_count,
                "schemas_used": schemas_used,
                "columns_pruned": columns_pruned,
                "error": None,
            }
            evaluation = evaluate_nl2sql_result(q, response_data)
            self_healed = had_error and evaluation.execution_success

            res = {
                "config": config_name,
                "id": q_id,
                "difficulty": diff,
                "success": evaluation.passed,
                "time_s": round(elapsed, 2),
                "schemas_used": "|".join(schemas_used),
                "columns_pruned": columns_pruned,
                "self_healed": self_healed,
            }
        except Exception as e:
            elapsed = time.perf_counter() - t0
            failed_response = {"success": False, "sql": "", "row_count": 0, "error": str(e)}
            evaluation = evaluate_nl2sql_result(q, failed_response)
            res = {
                "config": config_name,
                "id": q_id,
                "difficulty": diff,
                "success": False,
                "time_s": round(elapsed, 2),
                "schemas_used": "",
                "columns_pruned": 0,
                "self_healed": False,
            }

        status_str = "OK" if res["success"] else "FAIL"
        logger.info("  [%02d/%02d] [%s] %s", i, len(questions), diff.upper(), query[:60])
        logger.info("         %s  %.2fs | schemas=%s | pruned=%s", status_str, res["time_s"], res["schemas_used"], res["columns_pruned"])
        rows.append(res)
    return rows


def run_ablation_benchmark():
    logger.info("Khởi tạo hệ thống cho Ablation Study...")
    get_llm()
    ensure_index_ready()
    warmup_reranker()

    questions = load_questions()[:15]
    logger.info("Ablation Study: %d câu x %d configs = %d lượt chạy", len(questions), len(CONFIGS), len(questions) * len(CONFIGS))

    all_results = []

    for cfg in CONFIGS:
        cfg_name = cfg["name"]
        get_graph.cache_clear()

        patches = []
        if not cfg["reranker"]:
            patches.append(patch("src.chatbot.backend.agent.nodes.schema_retriever_node.rerank_schemas", side_effect=passthrough_reranker))
        if not cfg["pruner"]:
            patches.append(patch("src.chatbot.backend.agent.agents.retrieval_agent.column_pruner", side_effect=passthrough_column_pruner))

        for p in patches:
            p.start()

        try:
            rows = run_config_questions(cfg_name, questions)
            all_results.extend(rows)
        finally:
            for p in patches:
                p.stop()

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    print("\n" + "=" * 85)
    print(" ABLATION STUDY — Summary ".center(85, "="))
    print("=" * 85)
    print(f"  {'Config':<18} {'Total':>6} {'Success':>9} {'Exec Rate':>12} {'Avg Time':>12}")
    print("  " + "-" * 83)

    summary_rows = []
    for cfg in CONFIGS:
        cfg_name = cfg["name"]
        subset = [r for r in all_results if r["config"] == cfg_name]
        if not subset:
            continue
        total = len(subset)
        success = sum(1 for r in subset if r["success"])
        rate = (success / total) * 100
        avg_time = sum(r["time_s"] for r in subset) / total

        print(f"  {cfg_name:<18} {total:>6} {success:>9} {rate:>11.1f}% {avg_time:>11.2f}s")
        summary_rows.append({
            "config": cfg_name,
            "total": total,
            "success": success,
            "exec_rate": round(rate, 1),
            "avg_time_s": round(avg_time, 2),
        })

    print("=" * 85 + "\n")

    detail_csv = RESULTS_DIR / "ablation_detail.csv"
    fieldnames = ["config", "id", "difficulty", "success", "time_s", "schemas_used", "columns_pruned"]
    with open(detail_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in all_results:
            w.writerow({k: r[k] for k in fieldnames})

    summary_csv = RESULTS_DIR / "ablation_summary.csv"
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
        w.writeheader()
        w.writerows(summary_rows)

    logger.info("✅ Ablation benchmark saved → %s", summary_csv)
    return True


if __name__ == "__main__":
    raise SystemExit(0 if run_ablation_benchmark() else 1)
