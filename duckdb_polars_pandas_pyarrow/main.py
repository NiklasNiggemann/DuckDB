import os
import statistics
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
    times = []
    mem_usage = []
    for x in range(10):
        with utils.Timer() as timer:
            start = time.perf_counter()
            mem_before = get_memory_usage_mb()
            selected_function()
            mem_after = get_memory_usage_mb()
            end = time.perf_counter()
            mem_used = mem_after - mem_before
            times.append(end - start)
            mem_usage.append(mem_used)
            print(f"Memory used: {mem_used:.2f} MB")
            print(f"Elapsed time: {end - start:.4f} seconds")
    print("\n--- Benchmark Results ---")
    print(f"Median seconds elapsed: {statistics.median(times):.4f} s")
    print(f"Mean seconds elapsed:   {statistics.mean(times):.4f} s")
    print(f"Min seconds elapsed:    {min(times):.4f} s")
    print(f"Max seconds elapsed:    {max(times):.4f} s")
    print(f"Span seconds elapsed:   {max(times) - min(times):.4f} s\n")
    print(f"Median memory used:     {statistics.median(mem_usage):.2f} MB")
    print(f"Mean memory used:       {statistics.mean(mem_usage):.2f} MB")
    print(f"Min memory used:        {min(mem_usage):.2f} MB")
    print(f"Max memory used:        {max(mem_usage):.2f} MB")
    print(f"Span memory used:       {max(mem_usage) - min(mem_usage):.2f} MB")


main(duckdb_basics.purchases_and_count)
# main(polars_basics.purchases_and_count)
# main(pandas_basics.purchases_and_count)

