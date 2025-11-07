import os
import time
import utils
import psutil

def get_memory_usage_mb():
    process = psutil.Process(os.getpid())
    mem_bytes = process.memory_info().rss   # resident set size
    return mem_bytes / (1024 * 1024)        # convert to MB

def run_benchmark(selected_function):
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

