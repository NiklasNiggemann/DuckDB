import time
import psutil
import os
import duckdb_basics, polars_basics, pandas_basics

def get_memory_usage_mb() -> float:
    """Return current process memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)

def main():
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    duckdb_basics.purchases_and_count()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

if __name__ == "__main__":
    main()
