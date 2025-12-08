import os
import sys
import csv
import re
import subprocess
import statistics
import argparse
from typing import Optional, Tuple, List, Dict, Union
import plotter
import contextlib
import matplotlib.pyplot as plt

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

def export_results_csv(
    filename: str,
    tool: str,
    mode: str,
    memories: List[float],
    times: List[float]
) -> None:
    """Export benchmark results to a CSV file."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tool", "mode", "run", "memory_mb", "time_s"])
        for i, (mem, t) in enumerate(zip(memories, times), 1):
            writer.writerow([tool, mode, i, mem, t])

def parse_output(output: str) -> List[Tuple[float, float]]:
    """Parse output lines of the form 'Memory = X MB, Time = Y s'."""
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

def run_benchmark(
    n_runs: int,
    tool: str,
    mode: str
) -> Tuple[List[float], List[float]]:
    """Run the benchmark in either cold or hot mode."""
    memories, times = [], []
    args = [
        sys.executable, 'benchmark_engine.py',
        '--tool', tool,
        '--mode', mode
    ]
    if mode == "hot":
        args += ['--runs', str(n_runs)]

    try:
        if mode == "cold":
            # Run n times, each as a separate process
            for i in range(n_runs):
                print("\n------------------------------------------------")
                print(f"\n*** Run {i + 1}/{n_runs} ***\n")
                result = subprocess.check_output(args, stderr=subprocess.STDOUT)
                run_output = result.decode().strip()
                print(run_output)
                parsed = parse_output(run_output)
                if parsed:
                    mem, t = parsed[0]
                    memories.append(mem)
                    times.append(t)
                else:
                    print("Warning: Could not parse output!")
        else:
            # Hot mode: one process, multiple runs
            result = subprocess.check_output(args, stderr=subprocess.STDOUT)
            run_output = result.decode().strip()
            print(run_output)
            parsed = parse_output(run_output)
            if parsed:
                memories, times = zip(*parsed)
                memories, times = list(memories), list(times)
            else:
                print("Warning: Could not parse output!")
    except subprocess.CalledProcessError as e:
        print(f"Run failed: {e.output.decode()}")
    except subprocess.TimeoutExpired:
        print("Run timed out!")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{tool}_{mode}.csv", tool, mode, memories, times)
    return memories, times

def plot_multi(tools: List[str], mode: str) -> None:
    """Plot results for multiple tools."""
    files = [f"results/{b}_{mode}.csv" for b in tools]
    out_png = f"results/{'_'.join(tools)}_{mode}.png"
    plotter.plot_results_multi(files, True, out_png)

def main():
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing tools.")
    parser.add_argument("--tool", choices=["all", "duckdb_polars", "duckdb", "polars", "pandas"], required=True)
    parser.add_argument("--mode", choices=["all", "cold", "hot"], default="cold")
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()

    tool_map = {
        "all": ["pandas", "duckdb", "polars"],
        "duckdb_polars": ["duckdb", "polars"],
        "duckdb": ["duckdb"],
        "polars": ["polars"],
        "pandas": ["pandas"],
    }

    mode_map = {
        "all": ["cold", "hot"],
        "cold": ["cold"],
        "hot": ["hot"],
    }

    tools = tool_map[args.tool]
    modes = mode_map[args.mode]

    generated_csvs = []  # list of (tool, mode, path)
    print("\n=== Phase 1: Running benchmarks ===")
    for mode in modes:
        for tool in tools:
            print(f"\n[RUN] {mode} | {tool}")
            run_benchmark(args.runs, tool, mode)
            csv_path = f"results/{tool}_{mode}.csv"
            generated_csvs.append((tool, mode, csv_path))

    print("\n=== Phase 2: Plotting figures (saving to disk, no pop-ups) ===")
    with suppress_matplotlib_show():
        # Per-file line plots
        for tool, mode, csv_path in generated_csvs:
            if csv_has_data(csv_path):
                out_png = f"results/{tool}_{mode}.png"
                print(f"[PLOT] Line: {mode} | {tool} -> {out_png}")
                plotter.plot_results(
                    csv_path,
                    save_fig=True,
                    fig_name=out_png
                    # You can pass readability options if you adopted the improved plotter:
                    # smoothing_window=3, annotate_points=False, show_std_band=True, show_mean_line=True
                )
            else:
                print(f"[SKIP] No data in {csv_path} (line plot)")

        # Grouped bar charts (only if multiple tools)
        if len(tools) > 1:
            for mode in modes:
                csv_files = [f"results/{tool}_{mode}.csv" for tool in tools]
                existing = [p for p in csv_files if csv_has_data(p)]
                if existing:
                    out_png = f"results/{'_'.join(tools)}_{mode}_bar.png"
                    print(f"[PLOT] Bars: {mode} -> {out_png}")
                    plotter.barcharts(existing, save_fig=True, fig_name=out_png, tools=tools)
                else:
                    print(f"[SKIP] No data for bars: {mode}")

        # Hot vs Cold comparison (if both modes requested)
        if "cold" in modes and "hot" in modes:
            hot_cold_csvs = (
                    [f"results/{tool}_cold.csv" for tool in tools] +
                    [f"results/{tool}_hot.csv" for tool in tools]
            )
            existing = [p for p in hot_cold_csvs if csv_has_data(p)]
            if existing:
                out_png = f"results/{'_'.join(tools)}_hot_cold_bar.png"
                print(f"[PLOT] Hot vs Cold -> {out_png}")
                plotter.barcharts_hot_vs_cold(existing, save_fig=True, fig_name=out_png, tools=tools)
            else:
                print(f"[SKIP] No data for hot vs cold")

        # Multi-tool line graphs (only if multiple tools)
        if len(tools) > 1:
            for mode in modes:
                out_png = f"results/{'_'.join(tools)}_{mode}.png"
                print(f"[PLOT] Multi-line: {mode} -> {out_png}")
                plot_multi(tools, mode)

if __name__ == "__main__":
    main()

