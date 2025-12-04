import os
import shutil
import sys
import csv
import re
import subprocess
import statistics
import argparse
from typing import List, Tuple
from pathlib import Path
import duckdb
import plotter  # Your refactored plotter module
import contextlib
import matplotlib.pyplot as plt
import polars as pl

LOG_DIR = "results"
LOG_FILE = os.path.join(LOG_DIR, "benchmark_log.txt")
os.makedirs(LOG_DIR, exist_ok=True)

@contextlib.contextmanager
def suppress_matplotlib_show():
    """Temporarily suppress plt.show() so figures are saved but not displayed."""
    original_show = plt.show
    plt.show = lambda *args, **kwargs: None
    try:
        yield
    finally:
        plt.show = original_show

def csv_has_data(path: str) -> bool:
    """Check if CSV has at least one data row beyond header."""
    try:
        with open(path, newline="") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            first_row = next(reader, None)
            return first_row is not None
    except FileNotFoundError:
        return False

class Logger:
    """Logger that writes to both terminal and a file."""
    def __init__(self, filename: str):
        self.terminal = sys.stdout
        self.log = open(filename, "w", encoding="utf-8")
    def write(self, message: str):
        self.terminal.write(message)
        self.log.write(message)
    def flush(self):
        self.terminal.flush()
        self.log.flush()
    def close(self):
        self.log.close()

sys.stdout = Logger(LOG_FILE)
sys.stderr = sys.stdout  # Also log errors

def parse_output(output: str) -> List[Tuple[float, float]]:
    """Parse output lines of the form 'Memory = X MB, Time = Y s'."""
    import re
    pattern = re.compile(r"Memory\s*=\s*(-?[0-9.]+)\s*MB.*?Time\s*=\s*([0-9.]+)\s*s")
    return [(float(m), float(t)) for m, t in pattern.findall(output)]

def summarize(label: str, values: List[float]) -> None:
    print(f"\n--- {label} ---")
    if not values:
        print("No data.")
        return
    mean = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    cv = (std / mean) * 100 if mean else 0
    print(f"Mean:   {mean:.2f}")
    print(f"Std:    {std:.2f}")
    print(f"CV:     {cv:.1f}%")
    print(f"Min:    {min(values):.2f}")
    print(f"Max:    {max(values):.2f}")
    print(f"Span:   {max(values) - min(values):.2f}")

def export_results_csv(filename: str, tool: str, scale_factors: List[int], memories: List[float], times: List[float], sizes_mb: List[float], row_counts: List[int]) -> None:
    """Export benchmark results to CSV, including dataset size in MB and row count."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tool", "scale_factor", "memory_mb", "time_s", "dataset_size_mb", "row_count"])
        for sf, mem, t, sz, rc in zip(scale_factors, memories, times, sizes_mb, row_counts):
            writer.writerow([tool, sf, mem, t, sz, rc])

def run_benchmark(tool: str, scale_range: int) -> Tuple[List[int], List[float], List[float], List[float], List[int]]:
    scale_factors, memories, times, sizes_mb, row_counts = [], [], [], [], []
    try:
        for scale_factor in range(10, scale_range + 1, 10):
            print("\n------------------------------------------------\n")
            args = [sys.executable, 'benchmark_engine.py', '--tool', tool, '--scale_factor', str(scale_factor)]
            result = subprocess.check_output(args, stderr=subprocess.STDOUT)
            run_output = result.decode().strip()
            print(run_output)
            parsed = parse_output(run_output)
            if parsed:
                mem, t = parsed[0]
                scale_factors.append(scale_factor)
                memories.append(mem)
                times.append(t)
                # sizes_mb.append(dataset_size_mb)
                # row_counts.append(row_count)
            else:
                print("Warning: Could not parse output!")
    except subprocess.CalledProcessError as e:
        print(f"Run failed: {e.output.decode()}")
    except subprocess.TimeoutExpired:
        print("Run timed out!")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{tool}_scale-range_{scale_range}.csv", tool, scale_factors, memories, times, sizes_mb, row_counts)
    return scale_factors, memories, times, sizes_mb, row_counts

def main():

    parser = argparse.ArgumentParser(description="Benchmark runner for data processing tools.")
    parser.add_argument("--tool", choices=["all", "duckdb_polars", "duckdb", "polars", "pandas"], default="all")
    parser.add_argument("--scale_range", type=int, default=1)
    args = parser.parse_args()

    tool_map = {
        "all": ["pandas", "duckdb", "polars"],
        "duckdb_polars": ["duckdb", "polars"],
        "duckdb": ["duckdb"],
        "polars": ["polars"],
        "pandas": ["pandas"],
    }

    scale_range = args.scale_range
    tools = tool_map[args.tool]

    generated_csvs = []  # list of (tool, scale_range, path)
    print("\n=== Phase 1: Running benchmarks ===")
    for tool in tools:
        print(f"\n[START] {tool}")
        run_benchmark(tool, scale_range)
        csv_path = f"results/{tool}_{scale_range}.csv"
        generated_csvs.append((tool, scale_range, csv_path))

    print("\n=== Phase 2: Plotting figures (saving to disk) ===")
    with suppress_matplotlib_show():
        csv_files = [f"results/{tool}_scale-range_{scale_range}.csv" for tool in tools]
        existing = [p for p in csv_files if csv_has_data(p)]
        if existing:
            out_png = f"results/{'_'.join(tools)}_scale-range_{scale_range}.png"
            print(f"[PLOT] Memory vs Dataset Size: {scale_range} -> {out_png}")
            df = plotter.load_and_concat_csvs(existing)
            plotter.plot_memory_and_time_vs_dataset_size(df, tools=tools, save_fig=True, fig_name=out_png)
        else:
            print(f"[SKIP] No data")

if __name__ == "__main__":
    main()
