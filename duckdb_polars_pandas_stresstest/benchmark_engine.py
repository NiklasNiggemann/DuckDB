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
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def cold_benchmark(module) -> None:
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    module.total_sales_per_region()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB, Time = {end - start:.2f} s")

def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected tool and function.")
    parser.add_argument("--tool", choices=["duckdb", "polars", "pandas"], required=True)
    parser.add_argument("--scale_factor", type=int, default=1)
    args = parser.parse_args()

    tool_map = {
        "duckdb": duckdb_olap,
        "polars": polars_olap,
        "pandas": pandas_olap
    }
    module = tool_map[args.tool]
    cold_benchmark(module)

if __name__ == "__main__":
    main()
