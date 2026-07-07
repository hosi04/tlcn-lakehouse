import argparse
import csv
import logging
import time
from pathlib import Path

import requests
import yaml

from experiments.evaluation import evaluate_nl2sql_result, summarize_thresholds

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

API_BASE = "http://localhost:8000"
RESULTS_DIR = Path("experiments/results")
QUESTIONS_FILE = Path("experiments/nl2sql_test_questions.yaml")
EXPECTATIONS_FILE = Path("experiments/nl2sql_expectations.yaml")


def check_backend() -> bool:
    """Kiểm tra chatbot backend có sẵn sàng không."""
    try:
        resp = requests.get(f"{API_BASE}/health", timeout=5)
        data = resp.json()
        if data.get("status") == "ok":
            logger.info(
                "✅ Backend ready — %d schemas | %d SQL samples",
                data.get("schema_count", 0),
                data.get("sql_sample_count", 0),
            )
            return True
        else:
            logger.warning("⚠️  Backend running but index not ready (status=%s).", data.get("status"))
            logger.warning("    Gọi: POST %s/index-schema để index schema.", API_BASE)
            return False
    except Exception as e:
        logger.error("❌ Backend không phản hồi tại %s: %s", API_BASE, e)
        logger.error("   Chạy: uvicorn src.chatbot.backend.app:app --host 0.0.0.0 --port 8000")
        return False


def load_questions() -> list[dict]:
    with open(QUESTIONS_FILE, "r", encoding="utf-8") as f:
        questions = yaml.safe_load(f).get("questions", [])

    if EXPECTATIONS_FILE.exists():
        with open(EXPECTATIONS_FILE, "r", encoding="utf-8") as f:
            expectations = yaml.safe_load(f).get("expectations", {})
        for question in questions:
            question.update(expectations.get(question["id"], {}))

    return questions


def run_single_query(question: dict) -> dict:
    """Gọi API /chat và trả về kết quả đo lường."""
    q_id = question["id"]
    diff = question["difficulty"]
    query = question["query"]

    t0 = time.perf_counter()
    try:
        resp = requests.post(
            f"{API_BASE}/chat",
            json={"query": query, "session_id": f"bench_{q_id}"},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        elapsed = time.perf_counter() - t0

        sql = data.get("sql") or ""
        exec_log = data.get("execution_log", [])
        
        had_error = any(
            kw in str(log)
            for log in exec_log
            for kw in ("Lỗi", "fix", "Fix", "retry", "sửa", "FAILED", "attempt=2")
        )
        evaluation = evaluate_nl2sql_result(question, data)
        self_healed = had_error and evaluation.execution_success

        return {
            "id": q_id,
            "difficulty": diff,
            "query": query,
            "success": evaluation.execution_success,
            "passed": evaluation.passed,
            "has_sql": bool(sql),
            "sql": sql,
            "row_count": data.get("row_count", 0),
            "schemas_used": "|".join(data.get("schemas_used", [])),
            "columns_pruned": data.get("columns_pruned", 0),
            "had_error": had_error,
            "self_healed": self_healed,
            "table_score": evaluation.table_score,
            "keyword_score": evaluation.keyword_score,
            "forbidden_score": evaluation.forbidden_score,
            "eval_reason": evaluation.reason,
            "time_s": round(elapsed, 2),
            "error": None,
        }

    except Exception as e:
        elapsed = time.perf_counter() - t0
        failed_response = {"success": False, "sql": "", "row_count": 0, "error": str(e)}
        evaluation = evaluate_nl2sql_result(question, failed_response)
        return {
            "id": q_id,
            "difficulty": diff,
            "query": query,
            "success": False,
            "passed": evaluation.passed,
            "has_sql": False,
            "sql": "",
            "row_count": 0,
            "schemas_used": "",
            "columns_pruned": 0,
            "had_error": False,
            "self_healed": False,
            "table_score": evaluation.table_score,
            "keyword_score": evaluation.keyword_score,
            "forbidden_score": evaluation.forbidden_score,
            "eval_reason": evaluation.reason,
            "time_s": round(elapsed, 2),
            "error": str(e),
        }



def run_nl2sql_benchmark():
    parser = argparse.ArgumentParser(description="Run NL2SQL Benchmark")
    parser.add_argument("--resume", action="store_true", help="Tiếp tục chạy từ những câu chưa test")
    args = parser.parse_args()

    if not check_backend():
        return False

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = RESULTS_DIR / "nl2sql_benchmark.csv"

    questions = load_questions()
    
    done_ids = set()
    results = []
    
    if args.resume and csv_path.exists():
        valid_results = {}
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                passed = (str(row["passed"]).lower() == "true")
                row["passed"] = passed
                row["time_s"] = float(row.get("time_s", 0))
                # Ghi đè để giữ kết quả mới nhất nếu có duplicate
                valid_results[row["id"]] = row
        
        # Chỉ skip những câu đã PASS
        for k, v in valid_results.items():
            if v["passed"]:
                done_ids.add(k)
                results.append(v)
                
        logger.info(f"🔄 Đã load {len(done_ids)} câu hỏi đã PASS từ lần chạy trước. Sẽ chạy lại các câu chưa chạy hoặc bị FAIL.")

    questions_to_run = [q for q in questions if q["id"] not in done_ids]
    
    if not questions_to_run:
        logger.info("✅ Đã hoàn thành toàn bộ 100 câu hỏi! Chuyển sang in Summary...")
    else:
        logger.info("\n🚀 Bắt đầu NL2SQL Benchmark với %d câu hỏi...\n", len(questions_to_run))

    # Nếu resume, ta ghi đè file CSV bằng những câu đã PASS (xóa các dòng rác bị FAIL trước đó)
    mode = "w" if args.resume else "w"
    
    with open(csv_path, mode, newline="", encoding="utf-8") as f:
        writer = None
        
        # Ghi lại các câu đã PASS vào file CSV trước
        if args.resume and results:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        
        for i, q in enumerate(questions_to_run, 1):
            logger.info("[%02d/%02d] [%s] %s", i, len(questions_to_run), q["difficulty"].upper(), q["query"][:60])
            res = run_single_query(q)
            
            if writer is None:
                writer = csv.DictWriter(f, fieldnames=res.keys())
                if mode == "w":
                    writer.writeheader()
                    
            writer.writerow(res)
            f.flush()
            
            status = "✅" if res["passed"] else "❌"
            logger.info(
                "         %s  %.2fs | rows=%s | reason=%s | self_heal=%s",
                status, res["time_s"], res["row_count"], res["eval_reason"], res["self_healed"]
            )
            results.append(res)
            time.sleep(0.5)

    summary_lines = [
        "#" * 80,
        "# NL2SQL BENCHMARK — Summary",
        "#" * 80,
        f"| {'Difficulty':<12} | {'Total':>5} | {'Passed':>7} | {'Fail':>5} | "
        f"{'Pass Rate':>10} | {'Threshold':>10} | {'Status':>8} | {'Avg Time':>10} |",
        "|" + "-" * 14 + "|" + "-" * 7 + "|" + "-" * 9 + "|" + "-" * 7 + "|" + "-" * 12 + "|" + "-" * 12 + "|" + "-" * 10 + "|" + "-" * 12 + "|"
    ]

    threshold_ok, threshold_rows = summarize_thresholds(results)
    by_diff_threshold = {row["difficulty"]: row for row in threshold_rows}

    for diff in ["easy", "medium", "hard"]:
        subset = [r for r in results if r["difficulty"] == diff]
        if not subset:
            continue
        total = len(subset)
        passed = sum(1 for r in subset if r["passed"])
        fail = total - passed
        pass_rate = passed / total * 100
        threshold = by_diff_threshold[diff]["threshold"] * 100
        status = by_diff_threshold[diff]["status"]
        avg_time = sum(r["time_s"] for r in subset) / total

        summary_lines.append(
            f"| {diff.capitalize():<12} | {total:>5} | {passed:>7} | {fail:>5} | "
            f"{pass_rate:>9.1f}% | {threshold:>9.1f}% | {status:>8} | {avg_time:>9.2f}s |"
        )

    total_r = len(results)
    total_ok = sum(1 for r in results if r["passed"])
    total_time = sum(r["time_s"] for r in results) / total_r
    overall = by_diff_threshold["overall"]
    summary_lines.append("|" + "-" * 14 + "|" + "-" * 7 + "|" + "-" * 9 + "|" + "-" * 7 + "|" + "-" * 12 + "|" + "-" * 12 + "|" + "-" * 10 + "|" + "-" * 12 + "|")
    summary_lines.append(
        f"| **{'TOTAL':<10}** | **{total_r:>3}** | **{total_ok:>5}** | **{total_r - total_ok:>3}** | "
        f"**{total_ok / total_r * 100:>7.1f}%** | **{overall['threshold'] * 100:>7.1f}%** | "
        f"**{overall['status']:>6}** | **{total_time:>7.2f}s** |"
    )
    
    summary_text = "\n".join(summary_lines)
    print("\n" + summary_text + "\n")
    
    summary_path = RESULTS_DIR / "nl2sql_benchmark_summary.md"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary_text)

    logger.info("\n✅ NL2SQL benchmark saved → %s", csv_path)
    logger.info("✅ Summary saved → %s", summary_path)
    return threshold_ok


if __name__ == "__main__":
    raise SystemExit(0 if run_nl2sql_benchmark() else 1)
