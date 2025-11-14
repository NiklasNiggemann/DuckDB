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
        print("------------------------------------------------")
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
            else:
                print("Warning: Could not parse output!")
        except Exception as e:
            print(f"Run failed: {e}")
        finally:
            sys.stdout = old_stdout
    print("------------------------------------------------")
    print("Benchmark Finished!")
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
    print("Benchmarking Started (COLD)")
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
    print("------------------------------------------------")
    print("Benchmark Finished!")
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
    print("Benchmarking Started (HOT)")
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

def plot_results(output_file, save_fig=False, fig_name="benchmark_lines_stats.png"):
    """
    Plot execution time and memory usage per run for different backends and functions.

    Parameters:
        output_file (str): Path to the CSV file containing results.
        save_fig (bool): Whether to save the figure to a file.
        fig_name (str): Filename for saving the figure.
    """
    import pandas as pd
    import seaborn as sns
    import matplotlib.pyplot as plt
    import os

    # Load data
    if not os.path.isfile(output_file):
        raise FileNotFoundError(f"File not found: {output_file}")
    df = pd.read_csv(output_file)
    required_cols = {'backend', 'function', 'run', 'time_s', 'memory_mb'}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"CSV must contain columns: {required_cols}")

    # Prepare DataFrame
    sns.set(style="whitegrid", palette="muted", font_scale=1.2)
    df['label'] = df['backend'].astype(str) + ' | ' + df['function'].astype(str)
    df = df.sort_values(by=['label', 'run'])

    unique_labels = df['label'].unique()
    palette = sns.color_palette("husl", len(unique_labels))
    color_dict = dict(zip(unique_labels, palette))

    fig, axes = plt.subplots(2, 1, figsize=(14, 12), sharex=True)
    metrics = [
        ('time_s', 'Time (s)', 'Execution Time per Run'),
        ('memory_mb', 'Memory (MB)', 'Memory Usage per Run')
    ]

    for idx, (metric, ylabel, title) in enumerate(metrics):
        ax = axes[idx]
        for label in unique_labels:
            group = df[df['label'] == label]
            runs = group['run']
            values = group[metric]
            color = color_dict[label]

            # Plot line and points
            ax.plot(runs, values, marker='o', label=label, color=color, linewidth=2, markersize=7)

            # Mean, std, CV, min, max
            mean = values.mean()
            std = values.std()
            cv = std / mean if mean != 0 else float('nan')
            minv, maxv = values.min(), values.max()

            # Error band (mean ± std)
            ax.fill_between(runs, mean - std, mean + std, color=color, alpha=0.15)

            # Annotate mean and CV
            x_annot = runs.max() + 0.5
            ax.axhline(mean, linestyle='--', color=color, alpha=0.7)
            ax.text(
                x_annot, mean,
                f"mean={mean:.2f}\nCV={cv*100:.1f}%",
                va='center', ha='left', color=color, fontsize=11, fontweight='bold',
                bbox=dict(facecolor='white', edgecolor=color, boxstyle='round,pad=0.3', alpha=0.7)
            )

            # Annotate min and max
            ax.scatter(runs.loc[values.idxmin()], minv, color=color, marker='v', s=80, label=None)
            ax.scatter(runs.loc[values.idxmax()], maxv, color=color, marker='^', s=80, label=None)

        ax.set_title(title, fontsize=15)
        ax.set_ylabel(ylabel, fontsize=13)
        ax.legend(title='Backend | Function', bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=10)
        ax.grid(True, linestyle='--', alpha=0.5)

    axes[1].set_xlabel('Run', fontsize=13)
    plt.tight_layout()

    if save_fig:
        fig.savefig(fig_name, dpi=150, bbox_inches='tight')
        print(f"Figure saved as {fig_name}")

    plt.show()

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
    parser.add_argument("--backend", choices=["duckdb", "polars", "pandas"], required=True)
    parser.add_argument("--function", choices=[
        "filtering_and_counting",
        "filtering_grouping_aggregation",
        "grouping_and_conditional_aggregation"
    ], required=True)
    parser.add_argument("--mode", choices=["cold", "hot", "warm"], default="cold")
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=3, help="Number of warmup runs (for warm mode)")

    args = parser.parse_args()

    if args.mode == "cold":
        run_cold_benchmark(args.runs, args.backend, args.function, args.mode)
    elif args.mode == "hot":
        run_hot_benchmark(args.runs, args.backend, args.function, args.mode)
    elif args.mode == "warm":
        run_warm_benchmark(args.warmup, args.runs, args.backend, args.function, args.mode)

    print(f"\nBenchmark for {args.function} with {args.backend} in {args.mode} mode with {args.runs} runs.\n")
    plot_results(f"{args.backend}_{args.function}_{args.mode}.csv")

if __name__ == "__main__":
    main()
