import subprocess, statistics, re, sys
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

def benchmarking(n_runs, benchmark_target, memories, times):
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        from io import StringIO
        import sys
        old_stdout = sys.stdout
        sys.stdout = mystdout = StringIO()
        try:
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

def run_cold_benchmark(n_runs: int = 10):
    memories, times = [], []
    print("Benchmarking Started (COLD)")
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        try:
            result = subprocess.check_output(
                [sys.executable, 'benchmark_target.py'],
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

def run_hot_benchmark(n_runs: int = 10):
    import benchmark_target
    memories, times = [], []
    print("Benchmarking Started (HOT)")
    benchmarking(n_runs, benchmark_target, memories, times)

def run_warm_benchmark(n_warmup: int = 3, n_runs: int = 10):
    import benchmark_target
    print(f"Running {n_warmup} warmup runs (results ignored)...")
    for i in range(n_warmup):
        try:
            benchmark_target.main()
        except Exception as e:
            print(f"Warmup run {i+1} failed: {e}")
    print("Warmup complete. Starting measured runs.")
    memories, times = [], []
    benchmarking(n_runs, benchmark_target, memories, times)