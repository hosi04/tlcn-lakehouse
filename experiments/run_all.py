from __future__ import annotations

import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def _section(title: str):
    print("\n" + "█" * 80)
    print(f"  {title}")
    print("█" * 80)


def main():
    start = time.perf_counter()
    logger.info("🚀 Bắt đầu Thesis Benchmark Pipeline — %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    statuses = []

    _section("1/2  ETL PIPELINE BENCHMARK")
    try:
        from experiments.etl_benchmark import run_etl_benchmark
        statuses.append(("ETL", bool(run_etl_benchmark())))
    except Exception as e:
        logger.error("❌ ETL Benchmark thất bại: %s", e)
        statuses.append(("ETL", False))

    _section("2/2  NL2SQL CHATBOT BENCHMARK")
    try:
        from experiments.nl2sql_benchmark import run_nl2sql_benchmark
        statuses.append(("NL2SQL", bool(run_nl2sql_benchmark())))
    except Exception as e:
        logger.error("NL2SQL Benchmark that bai: %s", e)
        statuses.append(("NL2SQL", False))


    _section("3/4  ABLATION STUDY")
    try:
        from experiments.ablation_benchmark import run_ablation_benchmark
        statuses.append(("Ablation", bool(run_ablation_benchmark())))
    except Exception as e:
        logger.error("❌ Ablation Benchmark thất bại: %s", e)
        statuses.append(("Ablation", False))

    _section("4/4  ML MODEL BENCHMARK")
    try:
        from experiments.ml_benchmark import run_ml_benchmark
        statuses.append(("ML", bool(run_ml_benchmark())))
    except Exception as e:
        logger.error("❌ ML Benchmark thất bại: %s", e)
        statuses.append(("ML", False))

    elapsed = time.perf_counter() - start
    logger.info("\nBenchmark status:")
    for name, ok in statuses:
        logger.info("   %-10s %s", name, "PASS" if ok else "FAIL")

    all_ok = all(ok for _, ok in statuses)
    icon = "✅" if all_ok else "❌"
    logger.info("\n%s Toàn bộ benchmark kết thúc trong %.1f giây.", icon, elapsed)
    logger.info("   📁 Kết quả: experiments/results/")
    return all_ok


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
