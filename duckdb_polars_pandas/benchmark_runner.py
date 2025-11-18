"""
benchmark_runner.py

This module provides a command-line interface for benchmarking data processing functions
across different backends (DuckDB, Polars, Pandas). It supports cold, hot, and warm benchmarking modes,
measures memory usage and execution time, and summarizes the results.

Usage example:
    python benchmark_runner.py --backend duckdb --function filtering_and_counting --mode cold --runs 10 --output results.csv
"""

import csv
import subprocess
import statistics
import re
import sys
import argparse
from typing import Optional, Tuple, List
import duckdb_basics
import pandas_basics
import plotter
import polars_basics
import utils


def export_results_csv(filename, backend, function, mode, memories, times):
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["backend", "function", "mode", "run", "memory_mb", "time_s"])
        for i, (mem, t) in enumerate(zip(memories, times), 1):
            writer.writerow([backend, function, mode, i, mem, t])

def parse_output(output: str) -> Optional[Tuple[float, float]]:
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

def run_cold_benchmark(n_runs: int, backend: str, function: str, mode: str):
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
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"results/\nBenchmark for {function} with {backend} started!\n")

    backend_map = {
        "duckdb": duckdb_basics,
        "polars": polars_basics,
        "pandas": pandas_basics
    }
    mapped_backend = backend_map[backend]
    func = getattr(mapped_backend, function)

    import benchmark_target
    for i in range(n_runs):
        print("\n------------------------------------------------\n")
        print(f"Run {i + 1}/{n_runs}")
        with utils.suppress_stdout():
            mem, t = benchmark_target.run_hot_benchmark(func)
        memories.append(mem)
        times.append(t)
        print(f"Memory = {mem:.2f} MB, Time = {t:.2f} s")

    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)

def main():
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing backends.")
    parser.add_argument("--comparison", choices=["full", "duckdb_polars", None], default=None)
    parser.add_argument("--backend", choices=["duckdb", "polars", "pandas"])
    parser.add_argument("--function", choices=[
        "filtering_and_counting",
        "filtering_grouping_aggregation",
        "grouping_and_conditional_aggregation"
    ], required=True)
    parser.add_argument("--mode", choices=["cold", "hot"], default="cold")
    parser.add_argument("--runs", type=int, default=10)

    args = parser.parse_args()

    if args.comparison == "full":
        if args.mode == "cold":
            run_cold_benchmark(args.runs, "duckdb", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with DuckDB in {args.mode} mode with {args.runs} runs.\n")
            run_cold_benchmark(args.runs, "polars", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Polars in {args.mode} mode with {args.runs} runs.\n")
            run_cold_benchmark(args.runs, "pandas", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Pandas in {args.mode} mode with {args.runs} runs.\n")
            print(f"Benchmark-Comparison for {args.function} with DuckDB, Polars & Pandas in {args.mode} mode with {args.runs} runs finished.\n")
        if args.mode == "hot":
            run_hot_benchmark(args.runs, "duckdb", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with DuckDB in {args.mode} mode with {args.runs} runs.\n")
            run_hot_benchmark(args.runs, "polars", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Polars in {args.mode} mode with {args.runs} runs.\n")
            run_hot_benchmark(args.runs, "pandas", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Pandas in {args.mode} mode with {args.runs} runs.\n")
            print(f"Benchmark-Comparison for {args.function} with DuckDB, Polars & Pandas in {args.mode} mode with {args.runs} runs finished.\n")
        plotter.plot_results_multi([f"results/duckdb_{args.function}_{args.mode}.csv", f"polars_{args.function}_{args.mode}.csv", f"pandas_{args.function}_{args.mode}.csv"], True, f"results/duckdb_polars_pandas_{args.function}_{args.mode}.png")
        plotter.plot_results_multi([f"results/duckdb_{args.function}_{args.mode}.csv", f"polars_{args.function}_{args.mode}.csv"], True, f"results/duckdb_polars_{args.function}_{args.mode}.png")
    elif args.comparison == "duckdb_polars":
        if args.mode == "cold":
            run_cold_benchmark(args.runs, "duckdb", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with DuckDB in {args.mode} mode with {args.runs} runs.\n")
            run_cold_benchmark(args.runs, "polars", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Polars in {args.mode} mode with {args.runs} runs.\n")
            print(f"Benchmark-Comparison for {args.function} with DuckDB & Polars in {args.mode} mode with {args.runs} runs finished.\n")
        if args.mode == "hot":
            run_hot_benchmark(args.runs, "duckdb", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with DuckDB in {args.mode} mode with {args.runs} runs.\n")
            run_hot_benchmark(args.runs, "polars", args.function, args.mode)
            print(f"\nBenchmark-Results for {args.function} with Polars in {args.mode} mode with {args.runs} runs.\n")
            print(f"Benchmark-Comparison for {args.function} with DuckDB & Polars in {args.mode} mode with {args.runs} runs finished.\n")
        plotter.plot_results_multi([f"results/duckdb_{args.function}_{args.mode}.csv", f"polars_{args.function}_{args.mode}.csv"], True, f"results/duckdb_polars_{args.function}_{args.mode}.png")
    else:
        if args.mode == "cold":
            run_cold_benchmark(args.runs, args.backend, args.function, args.mode)
        elif args.mode == "hot":
            run_hot_benchmark(args.runs, args.backend, args.function, args.mode)
        print(f"\nBenchmark-Results for {args.function} with {args.backend} in {args.mode} mode with {args.runs} runs.\n")
        plotter.plot_results(f"results/{args.backend}_{args.function}_{args.mode}.csv", True, f"results/{args.backend}_{args.function}_{args.mode}.png")

if __name__ == "__main__":
    main()
