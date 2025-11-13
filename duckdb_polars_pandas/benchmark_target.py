import argparse

import psutil, time, os, gc
import duckdb_basics, polars_basics, pandas_basics

def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def run_benchmark(func):
    gc.collect()
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    func()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected backend and function.")
    parser.add_argument(
        "--backend",
        choices=["duckdb", "polars", "pandas"],
        required=True,
        help="The backend to run the benchmark on",
    )
    parser.add_argument(
        "--function",
        choices=["filtering_and_counting", "filtering_grouping_aggregation", "grouping_and_conditional_aggregation"],
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
