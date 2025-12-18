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
    if test == "stress":
        args += ['--factors'] + [str(f) for f in factor]
    elif test == "multiple":
        args += ['--num_files', str(factor)]
    else:
        args += ['--factor', str(factor)]
    try:
        result = subprocess.check_output(args, stderr=subprocess.STDOUT)
        run_output = result.decode().strip()
        print(run_output)
        parsed = parse_output(run_output)
        if parsed:
            mem, t = parsed[0]
            if test == "multiple":
                parquet_path = "tpc/lineitem_10.parquet"
                rc, sz = get_row_count_and_size(parquet_path)
                total_rows = rc * factor
                total_size = sz * factor
                return mem, t, total_rows, total_size, factor
            else:
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
            print(f"[NORMAL] Reading a table with ca. {factor * 0.22} GB ...")
            result = benchmark(tool, test, factor)
            if result:
                mem, t, rc, sz, scale = result
                memories.append(mem)
                times.append(t)
                row_counts.append(rc)
                sizes.append(sz)
                scales.append(scale)
    elif test == "stress":
        factor = 640
        memories, times, row_counts, sizes, scales = [], [], [], [], []
        for num_tables in (2, 4, 6, 8):
            factors = [factor] * num_tables
            print("\n------------------------------------------------\n")
            print(f"[STRESS] Reading {num_tables} tables (ca. {num_tables * 140} GB) as one file...")
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
        memories, times, row_counts, sizes, scales = [], [], [], [], []
        for num_files in (2, 4, 9, 18, 36, 72):
            print("\n------------------------------------------------\n")
            print(f"[MULTIPLE] Reading {num_files} files (ca. {num_files * 2} GB total)...")
            result = benchmark(tool, test, num_files)
            if result:
                mem, t, rc, sz, scale = result
                total_rows = rc * num_files
                total_size = sz * num_files
                memories.append(mem)
                times.append(t)
                row_counts.append(total_rows)
                sizes.append(total_size)
                scales.append(num_files)
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
    parser.add_argument("--test", choices=["all", "normal", "stress", "multiple"], default="normal")
    args = parser.parse_args()

    tool_map = {
        "all": ["polars", "duckdb"],
        "duckdb": ["duckdb"],
        "polars": ["polars"],
    }
    test_map = {
        "all": ["normal", "stress", "multiple"],
        "normal": ["normal"],
        "stress": ["stress"],
        "multiple": ["multiple"],
    }

    tools = tool_map[args.tool]
    tests = test_map[args.test]

    print("\n=== Phase 1: Running benchmarks ===")
    for tool in tools:
        for test in tests:
            print(f"\n[START] {tool} - {test}")
            run_benchmark(tool, test)

    print("\n=== Phase 2: Plotting figures (saving to disk) ===")
    with suppress_matplotlib_show():
        # Plot each test separately
        for test in tests:
            csv_files = [f"results/{tool}_{test}.csv" for tool in tools]
            existing = [p for p in csv_files if csv_has_data(p)]
            if existing:
                df = plotter.load_and_concat_csvs(existing)
                plotter.plot_scatter_with_trend(df, y_axis="memory_mb", save_path=f"results/{'_'.join(tools)}_{test}_memory.png")
                plotter.plot_scatter_with_trend(df, y_axis="time_s", save_path=f"results/{'_'.join(tools)}_{test}_time.png")
            else:
                print(f"[SKIP] No data for test {test}")

        # Plot all results together if --test all
        if args.test == "all":
            all_csvs = []
            for tool in tools:
                for test in test_map["all"]:
                    csv_path = f"results/{tool}_{test}.csv"
                    if csv_has_data(csv_path):
                        all_csvs.append(csv_path)
            if all_csvs:
                df = plotter.load_and_concat_csvs(all_csvs)
                plotter.plot_scatter_with_trend(df, y_axis="memory_mb", save_path=f"results/all_tools_all_tests_memory.png")
                plotter.plot_scatter_with_trend(df, y_axis="time_s", save_path=f"results/all_tools_all_tests_time.png")
            else:
                print("[SKIP] No data for combined plot")


if __name__ == "__main__":
    main()
