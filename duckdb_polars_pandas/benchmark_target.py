import psutil, time, os
import duckdb_basics, polars_basics, pandas_basics

def get_memory_usage_mb() -> float:
    # os.getpid() returns the process-ID of the current process
    # psutil.process represents an OS process
    process = psutil.Process(os.getpid())
    # Return current process memory usage in MB
    # memory_info() returns a tuple with variable fields depending on the platform
    # rss is the Resident Set Size and is the non-swapped physical memory a process has used
    # the rss is returned in bytes; 1 megabyte is equal to 1024^2 bytes
    return process.memory_info().rss / (1024 * 1024)

def main():
    start = time.perf_counter()
    mem_before = get_memory_usage_mb()
    # change function for individual benchmarks
    duckdb_basics.purchases_and_count()
    mem_after = get_memory_usage_mb()
    end = time.perf_counter()
    print(f"Memory = {mem_after - mem_before:.2f} MB")
    print(f"Time = {end - start:.2f} s")

if __name__ == "__main__":
    main()
