import csv
import subprocess
import statistics
import re
import sys
import argparse
from typing import Optional, Tuple, List
import plotter

def export_results_csv(
    filename: str,
    tool: str,
    function: str,
    mode: str,
    memories: List[float],
    times: List[float]
) -> None:
    """Export benchmark results to a CSV file."""
    with open(filename, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["tool", "function", "mode", "run", "memory_mb", "time_s"])
        for i, (mem, t) in enumerate(zip(memories, times), 1):
            writer.writerow([tool, function, mode, i, mem, t])

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
    function: str,
    mode: str
) -> Tuple[List[float], List[float]]:
    """Run the benchmark in either cold or hot mode."""
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"Benchmark for {function} with {tool} started!")
    args = [
        sys.executable, 'benchmark_engine.py',
        '--tool', tool,
        '--function', function,
        '--mode', mode
    ]
    if mode == "hot":
        args += ['--runs', str(n_runs)]

    try:
        if mode == "cold":
            # Run n times, each as a separate process
            for i in range(n_runs):
                print("\n------------------------------------------------\n")
                print(f"\n*** Run {i + 1}/{n_runs} ***\n")
                result = subprocess.check_output(args, stderr=subprocess.STDOUT)
                run_output = result.decode().strip()
                print(run_output)
                print(f"Results for Run {i+ 1} / {n_runs}")
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
    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {tool} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{tool}_{function}_{mode}.csv", tool, function, mode, memories, times)
    return memories, times

def plot_multi(tools: List[str], function: str, mode: str) -> None:
    files = [f"results/{b}_{function}_{mode}.csv" for b in tools]
    out_png = f"results/{'_'.join(tools)}_{function}_{mode}.png"
    plotter.plot_results_multi(files, True, out_png)

def main():
    parser = argparse.ArgumentParser(description="Benchmark runner for data processing tools.")
    parser.add_argument("--comparison", choices=["full", "duckdb_polars"], default=None)
    parser.add_argument("--tool", choices=["duckdb", "polars", "pandas"])
    parser.add_argument("--function", choices=[
        "filtering_counting",
        "filtering_grouping_aggregation",
        "grouping_conditional_aggregation"
    ], required=True)
    parser.add_argument("--mode", choices=["cold", "hot"], default="cold")
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()

    comparison_map = {
        "full": ["pandas", "duckdb", "polars"],
        "duckdb_polars": ["duckdb", "polars"]
    }

    if args.comparison:
        tools = comparison_map[args.comparison]
        for tool in tools:
            run_benchmark(args.runs, tool, args.function, args.mode)
        print(
            f"\nBenchmark-Comparison for {args.function} with {', '.join([b.capitalize() for b in tools])} in {args.mode} mode with {args.runs} runs finished.\n")
        plot_multi(tools, args.function, args.mode)
        if args.comparison == "full":
            plotter.barcharts(
                (f"results/{b}_{args.function}_{args.mode}.csv" for b in tools),
                True,
                f"results/{'_'.join(tools)}_{args.function}_{args.mode}_bar.png"
            )
            plot_multi(["duckdb", "polars"], args.function, args.mode)
    else:
        if not args.tool:
            parser.error("--tool is required if --comparison is not set")
        run_benchmark(args.runs, args.tool, args.function, args.mode)
        plotter.plot_results(
            f"results/{args.tool}_{args.function}_{args.mode}.csv",
            True,
            f"results/{args.tool}_{args.function}_{args.mode}.png"
        )

if __name__ == "__main__":
    main()
