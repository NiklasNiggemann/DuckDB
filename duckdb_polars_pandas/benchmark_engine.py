import argparse
from typing import Tuple, Any
import psutil
import time
import os
import gc
import duckdb_olap
import polars_olap
import pandas_olap
import utils


def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def cold_benchmark(func):
    gc.collect()
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    gc.collect()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

def hot_benchmark(func, n_runs) -> tuple[list[Any], list[Any]]:
    memories, times = [], []
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        start = time.perf_counter()
        mem_before = get_memory_usage_mb()
        with utils.suppress_stdout():
            func()
        mem_after = get_memory_usage_mb()
        end = time.perf_counter()
        time_tmp = end - start
        mem_tmp = mem_after - mem_before
        memories.append(mem_tmp)
        times.append(time_tmp)
        print(f"Memory = {mem_tmp:.2f} MB, Time = {time_tmp:.2f} s")
    return memories, times

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
            "filtering_counting",
            "filtering_grouping_aggregation",
            "grouping_conditional_aggregation"
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
        "duckdb": duckdb_olap,
        "polars": polars_olap,
        "pandas": pandas_olap
    }
    module = backend_map[args.backend]

    func = getattr(module, args.function)

    if args.mode == "cold":
        cold_benchmark(func)
    elif args.mode == "hot":
        hot_benchmark(func)

if __name__ == "__main__":
    main()
