import os
import time
import utils
import duckdb_basics
import polars_basics
import pandas_basics
from memory_profiler import profile
import psutil

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def get_memory_usage_mb():
    process = psutil.Process(os.getpid())
    mem_bytes = process.memory_info().rss   # resident set size
    return mem_bytes / (1024 * 1024)        # convert to MB

def main(selected_function):
    with utils.Timer() as timer:
        start = time.perf_counter()
        mem_before = get_memory_usage_mb()
        selected_function()
        mem_after = get_memory_usage_mb()
        end = time.perf_counter()
        mem_used = mem_after - mem_before
        elapsed = end - start
        print("\n--- Benchmark Results ---")
        print(f"Memory used: {mem_used:.2f} MB")
        print(f"Elapsed time: {elapsed:.4f} seconds")

# main(duckdb_basics.purchases_per_event_by_category)
main(polars_basics.purchases_and_count)
# main(pandas_basics.purchases_and_count)
