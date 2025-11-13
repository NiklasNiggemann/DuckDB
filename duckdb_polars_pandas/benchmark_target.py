"""
benchmark_target.py

This module provides a command-line interface for benchmarking data processing functions
across different backends (DuckDB, Polars, Pandas). It measures memory usage and execution time
for a selected function and backend, and prints the results in a parseable format.

Usage example:
    python benchmark_target.py --backend duckdb --function filtering_and_counting
"""

import argparse
import psutil
import time
import os
import gc
import duckdb_basics
import polars_basics
import pandas_basics

def get_memory_usage_mb() -> float:
    """
    Returns the current process memory usage in megabytes (MB).

    Returns:
        float: The resident set size (RSS) memory usage in MB.
    """
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def run_benchmark(func):
    """
    Runs the provided function while measuring memory usage and execution time.

    Args:
        func (callable): The function to benchmark.

    Prints:
        Memory used (MB) and time taken (seconds) in a parseable format.
    """
    gc.collect()
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

def main():
    """
    Parses command-line arguments to select the backend and function to benchmark,
    then runs the benchmark and prints the results.

    Command-line Arguments:
        --backend:   The backend to use ('duckdb', 'polars', or 'pandas').
        --function:  The function to benchmark
                     ('filtering_and_counting', 'filtering_grouping_aggregation',
                      or 'grouping_and_conditional_aggregation').
    """
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected backend and function.")
    parser.add_argument(
        "--backend",
        choices=["duckdb", "polars", "pandas"],
        required=True,
        help="The backend to run the benchmark on",
    )
    parser.add_argument(
        "--function",
        choices=[
            "filtering_and_counting",
            "filtering_grouping_aggregation",
            "grouping_and_conditional_aggregation"
        ],
        required=True,
        help="The function to run the benchmark on"
    )
    args = parser.parse_args()

    backend_map = {
        "duckdb": duckdb_basics,
        "polars": polars_basics,
        "pandas": pandas_basics
    }
    module = backend_map[args.backend]

    func = getattr(module, args.function)
    run_benchmark(func)

if __name__ == "__main__":
    main()
