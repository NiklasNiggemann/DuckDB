import argparse
import psutil
import time
import sys
import os
from typing import Any, List
import duckdb_olap
import polars_olap

LOG_DIR = "results"
LOG_FILE = os.path.join(LOG_DIR, "benchmark_log.txt")
os.makedirs(LOG_DIR, exist_ok=True)

class Logger:
    def __init__(self, filename: str):
        self.terminal = sys.stdout
        self.log = open(filename, "a", encoding="utf-8")
    def write(self, message: str):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()

sys.stdout = Logger(LOG_FILE)
sys.stderr = sys.stdout

def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def benchmark(tool: Any, test: str, factor, factors: List[int], num_files: int) -> None:
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    try:
        if test == "stress":
            tool.stress_test(factors)
        elif test == "multiple":
            tool.multiple_test(num_files)
        else:
            tool.normal_test(factor)
    except Exception as e:
        print(f"Error running benchmark: {e}")
        return
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB, Time = {end - start:.2f} s")

def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected tool and function.")
    parser.add_argument("--tool", choices=["duckdb", "polars"], required=True)
    parser.add_argument("--test", choices=["normal", "stress", "multiple"], default="normal")
    parser.add_argument("--factor", type=int)
    parser.add_argument("--factors", nargs="+", type=int)
    parser.add_argument("--num_files", type=int)
    args = parser.parse_args()

    tool_map = {
        "duckdb": duckdb_olap,
        "polars": polars_olap,
    }

    tool = tool_map[args.tool]
    benchmark(tool, args.test, args.factor, args.factors, args.num_files)

if __name__ == "__main__":
    main()
