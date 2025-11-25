import argparse
from typing import Callable, List, Any
import psutil
import time
import os
import duckdb_olap
import polars_olap
import pandas_olap
import utils

def get_memory_usage_mb() -> float:
    """Return current process memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def cold_benchmark(func: Callable[[], None]) -> None:
    """Run a single cold benchmark."""
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB, Time = {end - start:.2f} s")

def hot_benchmark(func: Callable[[], None], n_runs: int = 10) -> tuple[list[Any], list[Any]]:
    """Run a hot benchmark for n_runs."""
    memories, times = [], []
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        start = time.perf_counter()
        mem_before = get_memory_usage_mb()
        try:
            with utils.suppress_stdout():
                func()
        except Exception as e:
            print(f"Error in run {i + 1}: {e}", flush=True)
            break
        mem_after = get_memory_usage_mb()
        end = time.perf_counter()
        print(f"Memory = {mem_after - mem_before:.2f} MB, Time = {end - start:.2f} s")

def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected tool and function.")
    parser.add_argument("--tool", choices=["duckdb", "polars", "pandas"], required=True)
    parser.add_argument("--function", choices=[
        "filtering_counting",
        "filtering_grouping_aggregation",
        "grouping_conditional_aggregation"
    ], required=True)
    parser.add_argument("--mode", choices=["hot", "cold"], required=True)
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()

    tool_map = {
        "duckdb": duckdb_olap,
        "polars": polars_olap,
        "pandas": pandas_olap
    }
    module = tool_map[args.tool]
    func = getattr(module, args.function)

    if args.mode == "cold":
        cold_benchmark(func)
    else:
        hot_benchmark(func, args.runs)

if __name__ == "__main__":
    main()
