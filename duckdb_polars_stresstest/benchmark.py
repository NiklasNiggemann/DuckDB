import os
import sys
import csv
import subprocess
import statistics
import argparse
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

# === Utility Functions ===
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

def export_results_csv(
    filename: str,
    tool: str,
    scale_factors: List[int],
    memories: List[float],
    times: List[float],
    row_counts: List[int],
    sizes: List[float],
    test: str,
    tables: int = 1
) -> None:
    """Export benchmark results to a CSV file."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tool", "test", "scale_factor", "memory_mb", "time_s", "row_count", "dataset_size_mb"])
        for scale, mem, t, rc, sz in zip(scale_factors, memories, times, row_counts, sizes):
            writer.writerow([tool, test, scale, mem, t, rc, sz])

def get_row_count_and_size(parquet_path: str) -> Tuple[int, float]:
    """Get row count and file size (MB) for a Parquet file."""
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

# === Benchmarking Logic ===
def benchmark(tool: str, test: str, factor: Any) -> Optional[Tuple[float, float, int, float, Any]]:
    """Run a single benchmark and parse its output."""
    print("\n------------------------------------------------\n")
    args = [
        sys.executable, 'benchmark_engine.py',
        '--tool', tool,
        '--test', test,
    ]
    if test in {"stress-big", "stress-small"}:
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

def run_normal_benchmark(tool: str, scale_factors: List[int]) -> Tuple[List[float], List[float], List[int], List[float], List[int]]:
    """Run 'normal' benchmarks for a tool."""
    memories, times, row_counts, sizes, scales = [], [], [], [], []
    for factor in scale_factors:
        print("\n------------------------------------------------\n")
        print(f"[NORMAL] Reading a table with ca. {factor * 0.22} GB ...")
        mem_tmp, t_tmp = [], []
        rows = size = scale = 0
        for _ in range(10):
            print(f"Run {i+1} / 10")
            result = benchmark(tool, "normal", factor)
            if result:
                mem, t, rc, sz, sc = result
                mem_tmp.append(mem)
                t_tmp.append(t)
                rows, size, scale = rc, sz, sc
        memories.append(statistics.mean(mem_tmp))
        times.append(statistics.mean(t_tmp))
        row_counts.append(rows)
        sizes.append(size)
        scales.append(scale)
    return memories, times, row_counts, sizes, scales

def run_stress_benchmark(tool: str, test: str, factor: int, factor_range: Tuple[int, ...], gb_per_file: float) -> Tuple[List[float], List[float], List[int], List[float], List[int]]:
    """Run 'stress' benchmarks for a tool."""
    memories, times, row_counts, sizes, scales = [], [], [], [], []
    for factor_r in factor_range:
        factors = [factor] * factor_r
        print("\n------------------------------------------------\n")
        print(f"[{test.upper()}] Reading {factor_r} files with ca. {factor_r * gb_per_file:.2f} GB ...")
        mem_tmp, t_tmp = [], []
        rows = size = 0
        for i in range(10):
            print(f"Run {i+1} / 10")
            result = benchmark(tool, test, factors)
            if result:
                mem, t, rc, sz, _ = result
                mem_tmp.append(mem)
                t_tmp.append(t)
                rows, size = rc, sz
            else:
                print("Warning: Could not parse output!")
                break
        total_rows = rows * factor_r
        total_size = size * factor_r
        memories.append(statistics.mean(mem_tmp))
        times.append(statistics.mean(t_tmp))
        row_counts.append(total_rows)
        sizes.append(total_size)
        scales.append(factor_r)
    return memories, times, row_counts, sizes, scales

def run_benchmark(tool: str, test: str) -> Tuple[List[float], List[float]]:
    """Run benchmarks for a given tool and test type."""
    if test == "normal":
        scale_factors = [10, 20, 40, 80, 160, 320, 640]
        memories, times, row_counts, sizes, scales = run_normal_benchmark(tool, scale_factors)
    else:
        if test == "stress-big":
            factor, factor_range, gb_per_file = 640, (1, 2, 3, 4, 6, 8, 10), 137.49
        else:
            factor, factor_range, gb_per_file = 10, (1, 2, 4, 8, 18, 36, 72), 2.06
        memories, times, row_counts, sizes, scales = run_stress_benchmark(tool, test, factor, factor_range, gb_per_file)
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{tool}_{test}.csv", tool, scales, memories, times, row_counts, sizes, test)
    return memories, times

# === Main Entrypoint ===
def main():
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing tools.")
    parser.add_argument("--tool", choices=["all", "duckdb", "polars"], default="all")
    parser.add_argument("--test", choices=["all", "normal", "stress-big", "stress-small"], default="all")
    args = parser.parse_args()

    tool_map = {
        "all": ["polars", "duckdb"],
        "duckdb": ["duckdb"],
        "polars": ["polars"],
    }
    test_map = {
        "all": ["normal", "stress-small"],
        "normal": ["normal"],
        "stress-big": ["stress-big"],
        "stress-small": ["stress-small"],
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
        for test in tests:
            csv_files = [f"results/{tool}_{test}.csv" for tool in tools]
            existing = [p for p in csv_files if csv_has_data(p)]
            if existing:
                df = plotter.load_and_concat_csvs(existing)
                plotter.plot_scatter_with_trend(df, y_axis="memory_mb", save_path=f"results/{'_'.join(tools)}_{test}_memory.png")
                plotter.plot_scatter_with_trend(df, y_axis="time_s", save_path=f"results/{'_'.join(tools)}_{test}_time.png")

        if args.test == "all":
            overlay_csvs = [
                "results/duckdb_normal.csv",
                "results/duckdb_stress-small.csv",
                "results/polars_normal.csv",
                "results/polars_stress-small.csv",
            ]
            df = plotter.load_and_concat_csvs(overlay_csvs)
            plotter.plot_overlay_normal_stress(
                df,
                y_axis="memory_mb",
                save_path="results/polars_duckdb_overlay_memory.png"
            )
            plotter.plot_overlay_normal_stress(
                df,
                y_axis="time_s",
                save_path="results/polars_duckdb_overlay_time.png"
            )

if __name__ == "__main__":
    main()
