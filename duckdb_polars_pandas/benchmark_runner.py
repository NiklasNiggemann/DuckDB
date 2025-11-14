"""
benchmark_runner.py

This module provides a command-line interface for benchmarking data processing functions
across different backends (DuckDB, Polars, Pandas). It supports cold, hot, and warm benchmarking modes,
measures memory usage and execution time, and summarizes the results.

Usage example:
    python benchmark_runner.py --backend duckdb --function filtering_and_counting --mode cold --runs 10 --output results.csv
"""
import csv
import os
import subprocess
import statistics
import re
import sys
import argparse
from typing import Optional, Tuple, List

from matplotlib.pyplot import figure

import plotter

def export_results_csv(filename, backend, function, mode, memories, times):
    """
    Exports the results of a benchmark to a CSV file.
    """
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["backend", "function", "mode", "run", "memory_mb", "time_s"])
        for i, (mem, t) in enumerate(zip(memories, times), 1):
            writer.writerow([backend, function, mode, i, mem, t])

def parse_output(output: str) -> Optional[Tuple[float, float]]:
    """
    Parses the output string to extract memory and time values.

    Args:
        output (str): The output string from the benchmarked process.

    Returns:
        Optional[Tuple[float, float]]: A tuple of (memory in MB, time in seconds) if found, else None.
    """
    mem_match = re.search(r"Memory\s*=\s*([0-9.]+)\s*MB", output)
    time_match = re.search(r"Time\s*=\s*([0-9.]+)\s*s", output)
    if mem_match and time_match:
        return float(mem_match.group(1)), float(time_match.group(1))
    return None

def summarize(label: str, values: List[float]):
    if not values:
        print(f"\n--- {label} ---")
        print("No data.")
        return
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    cv = (std / mean) * 100 if mean != 0 else 0
    minv = min(values)
    maxv = max(values)
    span = maxv - minv
    print(f"\n--- {label} ---")
    print(f"Mean:   {mean:.2f}")
    print(f"Std:    {std:.2f}")
    print(f"CV:     {cv:.1f}%")
    print(f"Min:    {minv:.2f}")
    print(f"Max:    {maxv:.2f}")
    print(f"Span:   {span:.2f}")

def benchmarking(n_runs: int, backend: str, function: str, memories: List[float], times: List[float]):
    """
    Runs the benchmark in-process for the specified number of runs, capturing and parsing output.

    Args:
        n_runs (int): Number of benchmark runs.
        backend (str): Backend to benchmark ('duckdb', 'polars', 'pandas').
        function (str): Function to benchmark.
        memories (List[float]): List to store memory usage results.
        times (List[float]): List to store timing results.
    """
    import benchmark_target
    for i in range(n_runs):
        print("\n------------------------------------------------\n")
        print(f"Run {i+1}/{n_runs}")
        from io import StringIO
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        try:
            sys.argv = [
                "benchmark_target.py",
                "--backend", backend,
                "--function", function
            ]
            benchmark_target.main()
            run_output = mystdout.getvalue()
            print(run_output.strip())
            parsed = parse_output(run_output)
            if parsed:
                mem, t = parsed
                memories.append(mem)
                times.append(t)
                print(f"Memory = {mem:.2f} MB, Time = {t:.2f} s")
            else:
                print("Warning: Could not parse output!")
        except Exception as e:
            print(f"Run failed: {e}")
        finally:
            sys.stdout = old_stdout
    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)

def run_cold_benchmark(n_runs: int, backend: str, function: str, mode: str):
    """
    Runs the benchmark in a subprocess for each run (cold start), capturing and parsing output.

    Args:
        n_runs (int): Number of benchmark runs.
        backend (str): Backend to benchmark.
        function (str): Function to benchmark.
        mode (str): Mode to benchmark.
    """
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} started!\n")
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        try:
            result = subprocess.check_output(
                [sys.executable, 'benchmark_target.py',
                 '--backend', backend,
                 '--function', function],
                stderr=subprocess.STDOUT
            )
            run_output = result.decode().strip()
            parsed = parse_output(run_output)
            if parsed:
                mem, t = parsed
                memories.append(mem)
                times.append(t)
                print(f"Memory = {mem:.2f} MB, Time = {t:.2f} s")
            else:
                print("Warning: Could not parse output!")
        except subprocess.CalledProcessError as e:
            print(f"Run failed: {e.run_output.decode()}")
        except subprocess.TimeoutExpired:
            print("Run timed out!")
    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)

def run_hot_benchmark(n_runs: int, backend: str, function: str, mode: str):
    """
    Runs the benchmark in-process for the specified number of runs (hot start).

    Args:
        n_runs (int): Number of benchmark runs.
        backend (str): Backend to benchmark.
        function (str): Function to benchmark.
        mode (str): Mode to benchmark.
    """
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} started!\n")
    benchmarking(n_runs, backend, function, memories, times)
    export_results_csv(f"{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)

def run_warm_benchmark(n_warmup: int, n_runs: int, backend: str, function: str, mode: str):
    """
    Runs a specified number of warmup runs (output suppressed), then benchmarks as in hot mode.

    Args:
        n_warmup (int): Number of warmup runs (results ignored).
        n_runs (int): Number of measured benchmark runs.
        backend (str): Backend to benchmark.
        function (str): Function to benchmark.
        mode (str): Mode to benchmark.
    """
    import benchmark_target
    print(f"Running {n_warmup} warmup runs (results ignored)...")
    for i in range(n_warmup):
        try:
            sys.argv = [
                "benchmark_target.py",
                "--backend", backend,
                "--function", function
            ]
            # Suppress output during warmup
            with open(os.devnull, 'w') as devnull:
                old_stdout = sys.stdout
                old_stderr = sys.stderr
                sys.stdout = devnull
                sys.stderr = devnull
                try:
                    benchmark_target.main()
                finally:
                    sys.stdout = old_stdout
                    sys.stderr = old_stderr
        except Exception as e:
            pass
    print("Warmup complete. Starting measured runs.")
    memories, times = [], []
    benchmarking(n_runs, backend, function, memories, times)
    export_results_csv(f"{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)

def main():
    """
    Parses command-line arguments and runs the selected benchmark mode.

    Command-line Arguments:
        --backend:   The backend to use ('duckdb', 'polars', or 'pandas').
        --function:  The function to benchmark.
        --mode:      Benchmark mode ('cold', 'hot', or 'warm').
        --runs:      Number of measured runs.
        --warmup:    Number of warmup runs (for warm mode).
        --output:    CSV file to export raw results.
    """
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing backends.")
    parser.add_argument("--comparison", choices=["yes", None], default=None)
    parser.add_argument("--backend", choices=["duckdb", "polars", "pandas"])
    parser.add_argument("--function", choices=[
        "filtering_and_counting",
        "filtering_grouping_aggregation",
        "grouping_and_conditional_aggregation"
    ], required=True)
    parser.add_argument("--mode", choices=["cold", "hot", "warm"], default="cold")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=3, help="Number of warmup runs (for warm mode)")

    args = parser.parse_args()

    if args.comparison == "yes":
        run_cold_benchmark(args.runs, "duckdb", args.function, args.mode)
        print(f"\nBenchmark-Results for {args.function} with DuckDB in {args.mode} mode with {args.runs} runs.\n")
        run_cold_benchmark(args.runs, "polars", args.function, args.mode)
        print(f"\nBenchmark-Results for {args.function} with Polars in {args.mode} mode with {args.runs} runs.\n")
        run_cold_benchmark(args.runs, "pandas", args.function, args.mode)
        print(f"\nBenchmark-Results for {args.function} with Pandas in {args.mode} mode with {args.runs} runs.\n")
        print(f"Benchmark-Comparison for {args.function} with DuckDB, Polars & Pandas in {args.mode} mode with {args.runs} runs finished.\n")
        plotter.plot_results_multi([f"duckdb_{args.function}_{args.mode}.csv", f"polars_{args.function}_{args.mode}.csv", f"pandas_{args.function}_{args.mode}.csv"], True, f"duckdb_polars_pandas_{args.function}_{args.mode}.png")
    else:
        if args.mode == "cold":
            run_cold_benchmark(args.runs, args.backend, args.function, args.mode)
        elif args.mode == "hot":
            run_hot_benchmark(args.runs, args.backend, args.function, args.mode)
        elif args.mode == "warm":
            run_warm_benchmark(args.warmup, args.runs, args.backend, args.function, args.mode)
        print(f"\nBenchmark-Results for {args.function} with {args.backend} in {args.mode} mode with {args.runs} runs.\n")
        plotter.plot_results(f"{args.backend}_{args.function}_{args.mode}.csv", True, f"{args.backend}_{args.function}_{args.mode}.png")

if __name__ == "__main__":
    main()
