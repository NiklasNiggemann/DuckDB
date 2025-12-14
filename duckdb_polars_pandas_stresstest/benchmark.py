import os
import sys
import csv
import subprocess
import statistics
import argparse
from logging import exception
from typing import List, Tuple, Optional, Any
import duckdb
import plotter
import contextlib
import matplotlib.pyplot as plt

LOG_DIR = "results"
LOG_FILE = os.path.join(LOG_DIR, "benchmark_log.txt")
os.makedirs(LOG_DIR, exist_ok=True)

# === Utility Context Managers ===
@contextlib.contextmanager
def suppress_matplotlib_show():
    """Suppress plt.show() during plotting."""
    original_show = plt.show
    plt.show = lambda *args, **kwargs: None
    try:
        yield
    finally:
        plt.show = original_show

def csv_has_data(path: str) -> bool:
    """Check if a CSV file exists and has at least one data row."""
    try:
        with open(path, newline="") as f:
            reader = csv.reader(f)
            next(reader, None)  # Skip header
            return next(reader, None) is not None
    except FileNotFoundError:
        return False

def parse_output(output: str) -> List[Tuple[float, float]]:
    """Extract memory and time from benchmark output."""
    import re
    pattern = re.compile(r"Memory\s*=\s*(-?[0-9.]+)\s*MB.*?Time\s*=\s*([0-9.]+)\s*s")
    return [(float(m), float(t)) for m, t in pattern.findall(output)]

def summarize(label: str, values: List[float]) -> None:
    """Print summary statistics for a list of values."""
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
    print("\n------------------------------------------------")

def export_results_csv(filename: str, tool: str, scale_factors: List[int], memories: List[float], times: List[float], row_counts: List[int], sizes: List[float], tables: int = 1) -> None:
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tool", "scale_factor", "memory_mb", "time_s", "row_count", "dataset_size_mb"])
        for scale, mem, t, rc, sz in zip(scale_factors, memories, times, row_counts, sizes):
            writer.writerow([tool, scale, mem, t, rc, sz])

def get_row_count_and_size(parquet_path: str) -> Tuple[int, float]:
    try:
        rc = duckdb.sql(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        sz = os.path.getsize(parquet_path) / (1024 * 1024)  # MB
        return rc, sz
    except Exception as e:
        print(f"Error reading {parquet_path}: {e}")
        return 0, 0.0

# === Logger ===
class Logger:
    """Logger that writes to both stdout and a file."""
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
sys.stderr = sys.stdout

def benchmark(tool: str, test: str, factor: Any) -> Optional[Tuple[float, float, int, float, Any]]:
    print("\n------------------------------------------------\n")
    args = [
        sys.executable, 'benchmark_engine.py',
        '--tool', tool,
        '--test', test,
    ]
    if test == "stress-big" and isinstance(factor, list):
        args += ['--factors'] + [str(f) for f in factor]
    elif test == "stress-small" and isinstance(factor, list):
        args += ['--factors'] + [str(f) for f in factor]
    else:
        args += ['--factor', str(factor)]
    try:
        result = subprocess.check_output(args, stderr=subprocess.STDOUT)
        run_output = result.decode().strip()
        print(run_output)
        parsed = parse_output(run_output)
        if parsed:
            mem, t = parsed[0]
            parquet_path = f"tpc/lineitem_{factor if not isinstance(factor, list) else factor[0]}.parquet"
            rc, sz = get_row_count_and_size(parquet_path)
            return mem, t, rc, sz, factor
        else:
            print("Warning: Could not parse output!")
    except subprocess.CalledProcessError as e:
        print(f"Run failed: {e.output.decode()}")
    except subprocess.TimeoutExpired:
        print("Run timed out!")
    return None

def run_benchmark(tool: str, test: str) -> Tuple[List[float], List[float]]:
    memories, times, row_counts, sizes, scales = [], [], [], [], []
    if test == "normal":
        scale_factors = [10, 20, 40, 80, 160, 320, 640]
        for factor in scale_factors:
            print("\n------------------------------------------------\n")
            print(f"[NORMAL] Reading a table with factor {factor} for this run...")
            result = benchmark(tool, test, factor)
            if result:
                mem, t, rc, sz, scale = result
                memories.append(mem)
                times.append(t)
                row_counts.append(rc)
                sizes.append(sz)
                scales.append(scale)
    elif test == "stress-big":
        factor = 640
        memories, times, row_counts, sizes, scales = [], [], [], [], []
        for num_tables in range(10, 100, 10):
            factors = [factor] * num_tables
            print("\n------------------------------------------------\n")
            print(f"[STRESS-BIG] Reading {num_tables} tables for this run...")
            result = benchmark(tool, test, factors)
            if result:
                mem, t, rc, sz, scale = result
                total_rows = rc * num_tables
                total_size = sz * num_tables
                memories.append(mem)
                times.append(t)
                row_counts.append(total_rows)
                sizes.append(total_size)
                scales.append(num_tables)
            else:
                print("Warning: Could not parse output!")
                break
    else:
        factor = 10
        memories, times, row_counts, sizes, scales = [], [], [], [], []
        for num_tables in range(100, 900, 100):
            factors = [factor] * num_tables
            print("\n------------------------------------------------\n")
            print(f"[STRESS-SMALL] Reading {num_tables} tables for this run...")
            result = benchmark(tool, test, factors)
            if result:
                mem, t, rc, sz, scale = result
                total_rows = rc * num_tables
                total_size = sz * num_tables
                memories.append(mem)
                times.append(t)
                row_counts.append(total_rows)
                sizes.append(total_size)
                scales.append(num_tables)
            else:
                print("Warning: Could not parse output!")
                break
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{tool}_{test}.csv", tool, scales, memories, times, row_counts, sizes)
    return memories, times

def main():
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing tools.")
    parser.add_argument("--tool", choices=["all", "duckdb", "polars"], default="all")
    parser.add_argument("--test", choices=["normal", "stress-big", "stress-small"], default="normal")
    args = parser.parse_args()

    tool_map = {
        "all": ["polars", "duckdb"],
        "duckdb": ["duckdb"],
        "polars": ["polars"],
    }
    tools = tool_map[args.tool]

    print("\n=== Phase 1: Running benchmarks ===")
    for tool in tools:
        print(f"\n[START] {tool}")
        run_benchmark(tool, args.test)

    print("\n=== Phase 2: Plotting figures (saving to disk) ===")
    with suppress_matplotlib_show():
        csv_files = [f"results/{tool}_{args.test}.csv" for tool in tools]
        existing = [p for p in csv_files if csv_has_data(p)]
        if existing:
            df = plotter.load_and_concat_csvs(existing)
            plotter.plot_scatter_with_trend(df, y_axis="memory_mb", save_path=f"results/{'_'.join(tools)}_{args.test}_memory.png")
            plotter.plot_scatter_with_trend(df, y_axis="time_s", save_path=f"results/{'_'.join(tools)}_{args.test}_time.png")
        else:
            print(f"[SKIP] No data")

if __name__ == "__main__":
    main()
