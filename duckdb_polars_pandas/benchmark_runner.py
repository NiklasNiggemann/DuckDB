import os
import subprocess, statistics, re, sys, argparse
from typing import Optional, Tuple, List

def parse_output(output: str) -> Optional[Tuple[float, float]]:
    # extracts memory and time values from the output string using regex
    mem_match = re.search(r"Memory\s*=\s*([0-9.]+)\s*MB", output)
    time_match = re.search(r"Time\s*=\s*([0-9.]+)\s*s", output)
    if mem_match and time_match:
        # returns tuple of memory & time
        return float(mem_match.group(1)), float(time_match.group(1))
    return None

def summarize(label: str, values: List[float]):
    print(f"\n--- {label} ---")
    print(f"Median: {statistics.median(values):.2f}")
    print(f"Min:    {min(values):.2f}")
    print(f"Max:    {max(values):.2f}")
    print(f"Span:   {max(values) - min(values):.2f}")

def benchmarking(n_runs, backend, function, memories, times):
    import importlib
    # Dynamically import benchmark_target with CLI args
    import benchmark_target
    import io
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
            output = mystdout.getvalue()
            print(output.strip())
            parsed = parse_output(output)
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

def run_cold_benchmark(n_runs, backend, function):
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
            output = result.decode().strip()
            parsed = parse_output(output)
            if parsed:
                mem, t = parsed
                memories.append(mem)
                times.append(t)
                print(f"Memory = {mem:.2f} MB, Time = {t:.2f} s")
            else:
                print("Warning: Could not parse output!")
        except subprocess.CalledProcessError as e:
            print(f"Run failed: {e.output.decode()}")
        except subprocess.TimeoutExpired:
            print("Run timed out!")
    print("------------------------------------------------")
    print("Benchmark Finished!")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)

def run_hot_benchmark(n_runs, backend, function):
    memories, times = [], []
    print("Benchmarking Started (HOT)")
    benchmarking(n_runs, backend, function, memories, times)

def run_warm_benchmark(n_warmup, n_runs, backend, function):
    import benchmark_target
    print(f"Running {n_warmup} warmup runs (results ignored)...")
    for i in range(n_warmup):
        try:
            sys.argv = [
                "benchmark_target.py",
                "--backend", backend,
                "--function", function
            ]
            # Redirect stdout and stderr to suppress output
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
            # Optionally, you can log warmup errors if you want
            pass
    print("Warmup complete. Starting measured runs.")
    memories, times = [], []
    benchmarking(n_runs, backend, function, memories, times)

def main():
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
        run_cold_benchmark(args.runs, args.backend, args.function)
    elif args.mode == "hot":
        run_hot_benchmark(args.runs, args.backend, args.function)
    elif args.mode == "warm":
        run_warm_benchmark(args.warmup, args.runs, args.backend, args.function)

if __name__ == "__main__":
    main()