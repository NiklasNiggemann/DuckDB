import argparse
from typing import Tuple

import psutil
import time
import os
import gc
import duckdb_basics
import polars_basics
import pandas_basics

def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def run_cold_benchmark(func):
    gc.collect()
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    gc.collect()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

def run_hot_benchmark(func) -> Tuple[float, float]:
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    return (mem_after - mem_before), (end - start)

def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected backend and function.")
    parser.add_argument(
        "--backend",
        choices=["duckdb", "polars", "pandas"],
        required=True,
    )
    parser.add_argument(
        "--function",
        choices=[
            "filtering_and_counting",
            "filtering_grouping_aggregation",
            "grouping_and_conditional_aggregation"
        ],
        required=True,
    )
    parser.add_argument(
        "--mode",
        choices=["hot", "cold"],
        required=True,
    )
    args = parser.parse_args()

    backend_map = {
        "duckdb": duckdb_basics,
        "polars": polars_basics,
        "pandas": pandas_basics
    }
    module = backend_map[args.backend]

    func = getattr(module, args.function)

    if args.mode == "cold":
        run_cold_benchmark(func)
    elif args.mode == "hot":
        run_hot_benchmark(func)

if __name__ == "__main__":
    main()
