import subprocess, statistics, re, sys
from typing import Optional, Tuple, List

N_RUNS = 10

def parse_output(output: str) -> Optional[Tuple[float, float]]:
    # extracts memory and time values from the output string using regex
    mem_match = re.search(r"Memory\s*=\s*([0-9.]+)\s*MB", output)
    time_match = re.search(r"Time\s*=\s*([0-9.]+)\s*s", output)
    if mem_match and time_match:
        # returns tuple of memory & time
        return float(mem_match.group(1)), float(time_match.group(1))
    return None

def run_benchmark(n_runs: int) -> Tuple[List[float], List[float]]:
    memories, times = [], []
    print("Benchmarking Started")
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        try:
            result = subprocess.check_output(
                [sys.executable, 'benchmark_target.py'],
                stderr=subprocess.STDOUT
            )
            output = result.decode().strip()
            print(output)
            parsed = parse_output(output)
            if parsed:
                mem, t = parsed
                memories.append(mem)
                times.append(t)
            else:
                print("Warning: Could not parse output!")
        except subprocess.CalledProcessError as e:
            print(f"Run failed: {e.output.decode()}")
        except subprocess.TimeoutExpired:
            print("Run timed out!")
    print("------------------------------------------------")
    print("Benchmark Finished!")
    return memories, times

def summarize(label: str, values: List[float]):
    print(f"\n--- {label} ---")
    print(f"Median: {statistics.median(values):.2f}")
    print(f"Min:    {min(values):.2f}")
    print(f"Max:    {max(values):.2f}")
    print(f"Span:   {max(values) - min(values):.2f}")

def main():
    memories, times = run_benchmark(N_RUNS)
    if times and memories:
        summarize("Elapsed Time (s)", times)
        summarize("Memory Used (MB)", memories)
    else:
        print("No valid results collected.")

if __name__ == "__main__":
    main()
