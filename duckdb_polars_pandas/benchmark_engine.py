import argparse
from typing import Callable, List, Any
import psutil
import time
import duckdb_olap
import polars_olap
import pandas_olap
import sys
import os

LOG_DIR = "results"
LOG_FILE = os.path.join(LOG_DIR, "benchmark_log.txt")

# Ensure log directory exists
os.makedirs(LOG_DIR, exist_ok=True)

class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")
    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(LOG_FILE)
sys.stderr = sys.stdout  # Optional: also log errors


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

def hot_benchmark(func: Callable[[], None], n_runs: int = 10) -> None:
    """Run a hot benchmark for n_runs."""
    memories, times = [], []
    for i in range(n_runs):
        print("\n------------------------------------------------\n")
        print(f"\n*** Run {i+1}/{n_runs} ***\n")
        start = time.perf_counter()
        mem_before = get_memory_usage_mb()
        try:
            func()
        except Exception as e:
            print(f"Error in run {i + 1}: {e}", flush=True)
            break
        mem_after = get_memory_usage_mb()
        end = time.perf_counter()
        print(f"Memory = {mem_after - mem_before:.2f} MB, Time = {end - start:.2f} s")
        print(f"Memory Profiler for Run {i + 1}/{n_runs}")


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
