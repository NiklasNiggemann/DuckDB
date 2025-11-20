# DuckDB vs Polars vs Pandas: A Modern DataFrame Benchmark

## Overview

This project benchmarks the performance of leading DataFrame and analytical query engines in Python: **DuckDB**, **Polars**, and **Pandas**. Each tool represents a distinct approach to tabular analytics:

- **DuckDB**: An in-process SQL OLAP database, similar to SQLite but optimized for analytical workloads.
- **Polars**: A modern, high-performance DataFrame library written in Rust, designed for speed and efficiency.
- **Pandas**: The most widely used DataFrame library in Python, known for its rich API and extensive ecosystem.

This benchmarking project was created for its accompanying blog article for codecentric – refer to that article for more background information, discussion of results etc.

### Dataset

The [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset is used for these benchmarks. It is a real-world dataset with:

- **Size:** 9 GB
- **Rows:** 67,501,979
- **Columns:** 18

This scale is representative of production analytics scenarios.

---

### Benchmarking Methodology

Benchmarks are conducted using a robust, **modular, and fully CLI-driven** setup. The framework supports three benchmarking modes:

- **Cold:** Each run is executed in a new subprocess, eliminating caching effects and simulating a "cold start."
- **Hot:** All runs are executed in the same process, measuring performance with potential caching effects.

Each operation is executed multiple times (default: 10), and the median, minimum, maximum, and span (max - min) are reported for both execution time (in seconds) and memory usage (in MB).

**Test Environment:**  
- MacBook Pro (2021), Apple M1 Max, 32 GB RAM  
- Python environment as specified in the repository

---

### Benchmarked Operations

Three fundamental OLAP operations are evaluated:

1. **Filtering & Counting**  
   Select rows based on a condition and count them.
2. **Filtering, Grouping & Aggregation**  
   Filter rows, group by a category, and aggregate a numeric column.
3. **Grouping & Conditional Aggregation**  
   Group by category and count views, carts, and purchases for each category.

---

## Setup 

The repository can simply be copied via the GitHub functionality. Dependencies of the project can be installed via the `requirements.txt`:

```ssh
pip install -r requirements.txt

```

The dataset `.csv` is to be placed in a different dirctory. This perhaps unusual, bothersome setup stems from the fact that this project is part of multiple testing, benchmarking and playground projects that all utizile a dataset directory to share the same datasets. 

```
- duckdb_polars_pandas 
- dataset
-- ecommerce.csv 
```

---

## How to Run Benchmarks

The benchmarking framework is fully parameterized via the command line.  You can select the backend, operation, mode, and number of runs without editing any code.

### **Example CLI Usage**

```sh
# Cold run, 10 times, DuckDB, filtering_and_counting
python benchmark.py --backend duckdb --function filtering_counting --mode cold --runs 10

# Hot run, 5 times, Polars, filtering_grouping_aggregation
python benchmark.py --backend polars --function filtering_grouping_aggregation --mode hot --runs 5

# Warm run, 3 warmups, 7 runs, Pandas, grouping_and_conditional_aggregation
python benchmark.py --backend pandas --function grouping_conditional_aggregation --mode warm --runs 7 --warmup 3
```

**Available options:**
- `--backend`: `duckdb`, `polars`, or `pandas`
- `--function`:  
    - `filtering_counting`  
    - `filtering_grouping_aggregation`  
    - `grouping_conditional_aggregation`
- `--mode`: `cold` or `hot` 
- `--runs`: Number of measured runs (default: 10) 

Additonally, instead of a specific backend for a singular benchmark, we can toggle a comparison mode:
- `--comparison`: `full`, `duckdb_polars`

`full`runs the selected function and mode on all backends after one another. `duckdb_polars`only uses DuckDB and Polars – Note: `full` exports a comparison of DuckDB and Polars automatically as well.

```sh
# Cold run, 10 times, comparing DuckDB, Polars and Pandas 
python benchmark.py --comparison full --function filtering_counting --mode cold --runs 10

# Hot run, 5 times, comparing DuckDB and Polars 
python benchmark.py --comparison duckdb_polars --function filtering_grouping_aggregation --mode hot --runs 5

```

Certainly! Here’s an improved, clearer, and more structured version of your code explanation. I’ve preserved all code and technical details, but enhanced readability, flow, and clarity. No code has been deleted.

---

## Code Explanation

The main module of this project is `benchmark.py`. This script orchestrates the benchmarking of different data processing backends (DuckDB, Polars, Pandas) via a command-line interface (CLI).

### Argument Parsing

First, we define the CLI arguments using Python’s `argparse` module:

```python
parser = argparse.ArgumentParser(description="Benchmark runner for data processing backends.")
parser.add_argument("--comparison", choices=["full", "duckdb_polars", None], default=None)
parser.add_argument("--backend", choices=["duckdb", "polars", "pandas"])
parser.add_argument("--function", choices=[
   "filtering_counting",
   "filtering_grouping_aggregation",
   "grouping_conditional_aggregation"
], required=True)
parser.add_argument("--mode", choices=["cold", "hot"], default="cold")
parser.add_argument("--runs", type=int, default=10)
```

### Benchmark Execution Logic

Based on the CLI input, the script determines which backends and functions to benchmark:

```python
comparison_map = {
   "full": ["duckdb", "polars", "pandas"],
   "duckdb_polars": ["duckdb", "polars"]
}

if args.comparison in comparison_map:
   backends = comparison_map[args.comparison]
   for backend in backends:
      initialize_benchmark(args.runs, backend, args.function, args.mode)
   print(
      f"\nBenchmark-Comparison for {args.function} with {', '.join([b.capitalize() for b in backends])} in {args.mode} mode with {args.runs} runs finished.\n")
   plot_multi(backends, args.function, args.mode)
   if args.comparison == "full":
      plot_multi(["duckdb", "polars"], args.function, args.mode)
else:
   initialize_benchmark(args.runs, args.backend, args.function, args.mode)
   plotter.plot_results(
      f"results/{args.backend}_{args.function}_{args.mode}.csv",
      True,
      f"results/{args.backend}_{args.function}_{args.mode}.png"
   )
```

### Benchmark Initialization

The `initialize_benchmark` function triggers the selected benchmark mode:

```python
def initialize_benchmark(runs, backend, function, mode):
    if mode == "cold":
        run_cold_benchmark(runs, backend, function, mode)
    elif mode == "hot":
        run_hot_benchmark(runs, backend, function, mode)
    print(f"\nBenchmark-Results for {function} with {backend.capitalize()} in {mode} mode with {runs} runs.")
```

### Hot Benchmarking

The hot benchmark runs the selected function multiple times in a loop, using the `hot_benchmark` function from `benchmark_engine.py`:

```python
def run_hot_benchmark(n_runs: int, backend: str, function: str, mode: str):
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"Benchmark for {function} with {backend} started!")
    backend_map = {
        "duckdb": duckdb_olap,
        "polars": polars_olap,
        "pandas": pandas_olap
    }
    mapped_backend = backend_map[backend]
    func = getattr(mapped_backend, function)
    import benchmark_engine
    for i in range(n_runs):
        print("\n------------------------------------------------\n")
        print(f"Run {i + 1}/{n_runs}")
        with utils.suppress_stdout():
            mem, t = benchmark_engine.run_hot_benchmark(func)
        memories.append(mem)
        times.append(t)
        print(f"Memory = {mem:.2f} MB, Time = {t:.2f} s")
    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)
```

#### Suppressing Output

To keep the terminal output clean, the `suppress_stdout` context manager from `utils.py` is used:

```python
@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
```

### Cold Benchmarking

The cold benchmark uses Python’s subprocess management to avoid caching effects:

```python
def run_cold_benchmark(n_runs: int, backend: str, function: str, mode: str):
    memories, times = [], []
    print("\n-----------------------------------------------\n")
    print(f"Benchmark for {function} with {backend} started!")
    for i in range(n_runs):
        print("------------------------------------------------")
        print(f"Run {i+1}/{n_runs}")
        try:
            result = subprocess.check_output(
                [sys.executable, 'benchmark_engine.py',
                 '--backend', backend,
                 '--function', function,
                 '--mode', mode],
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
    print("\n------------------------------------------------\n")
    print(f"\nBenchmark for {function} with {backend} finished!\n")
    summarize("Elapsed Time (s)", times)
    summarize("Memory Used (MB)", memories)
    export_results_csv(f"results/{backend}_{function}_{mode}.csv", backend, function, mode, memories, times)
```

#### Output Parsing

Since the cold benchmark runs in a separate process, its output is parsed:

```python
def parse_output(output: str) -> Optional[Tuple[float, float]]:
    mem_match = re.search(r"Memory\s*=\s*([0-9.]+)\s*MB", output)
    time_match = re.search(r"Time\s*=\s*([0-9.]+)\s*s", output)
    if mem_match and time_match:
        return float(mem_match.group(1)), float(time_match.group(1))
    return None
```

### Result Summarization

After each benchmark, the `summarize` function prints statistics such as mean, standard deviation, min, max, and coefficient of variation (CV):

```ssh
--- Elapsed Time (s) ---
Mean:   2.32
Std:    0.05
CV:     2.0%
Min:    2.25
Max:    2.38
Span:   0.13

--- Memory Used (MB) ---
Mean:   28.80
Std:    65.23
CV:     226.5%
Min:    1.02
Max:    210.92
Span:   209.91
```

During benchmarking, progress updates are printed for each run:

```ssh
Benchmark for filtering_grouping_aggregation with polars started!
------------------------------------------------
Run 1/10
Memory = 12425.55 MB, Time = 2.92 s
------------------------------------------------
Run 2/10
Memory = 12426.66 MB, Time = 2.94 s
------------------------------------------------
```

### Benchmark Engine

The `benchmark_engine.py` module is invoked by the cold benchmark subprocess. Its `main` function is similar to that of `benchmark.py`:

```python
def main():
    parser = argparse.ArgumentParser(description="Run a benchmark on a selected backend and function.")
    parser.add_argument(
        "--backend",
        choices=["duckdb", "polars", "pandas"],
        required=True,
    )
    parser.add_argument(
        "--function",
        choices=[
            "filtering_counting",
            "filtering_grouping_aggregation",
            "grouping_conditional_aggregation"
        ],
        required=True,
    )
    parser.add_argument(
        "--mode",
        choices=["hot", "cold"],
        required=True,
    )
    args = parser.parse_args()

    backend_map = {
        "duckdb": duckdb_olap,
        "polars": polars_olap,
        "pandas": pandas_olap
    }
    module = backend_map[args.backend]

    func = getattr(module, args.function)

    if args.mode == "cold":
        cold_benchmark(func)
    elif args.mode == "hot":
        hot_benchmark(func)
```

#### Measuring Time and Memory

Both `cold_benchmark` and `hot_benchmark` simply execute the function and measure time and memory usage. Memory is measured using `psutil`:

```python
def get_memory_usage_mb() -> float:
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / (1024 * 1024)
```

**Note:** Memory usage is measured before and after the function runs, so negative values indicate memory was freed during execution.

---

## OLAP Implementation

The project uses a shared dataset, accessed via a helper function in `utils.py`:

```python
def get_dataset_dir():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    datasets = os.path.join(current_dir, '..', 'datasets')
    return os.path.normpath(datasets)
```

The dataset path is then used in each backend’s OLAP implementation:

```python
dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"
```

### DuckDB

```python
def filtering_counting():
    duckdb.sql(
        f"SELECT * FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()
    duckdb.sql(
        f"SELECT COUNT(*) AS purchase_count FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()

def filtering_grouping_aggregation():
    duckdb.sql(
        f"""
        SELECT category_code, SUM(price) AS total_sales
        FROM read_csv_auto('{dataset_path}')
        WHERE event_type = 'purchase'
        GROUP BY category_code
        """
    ).show()

def grouping_conditional_aggregation():
    duckdb.sql(
        f"""
        SELECT
            category_code,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
        FROM read_csv_auto('{dataset_path}')
        GROUP BY category_code
        """
    ).show()
```

### Polars

```python
def filtering_counting():
    df = pl.read_csv(dataset_path)
    purchases = df.filter(pl.col("event_type") == "purchase")
    print(purchases)
    print("Count:", purchases.height)

def filtering_grouping_aggregation():
    df = pl.read_csv(dataset_path)
    result = (
        df.filter(pl.col("event_type") == "purchase")
        .group_by("category_code")
        .agg(pl.col("price").sum().alias("total_sales"))
    )
    print(result)

def grouping_conditional_aggregation():
    df = pl.read_csv(dataset_path)
    result = (
        df.group_by("category_code")
        .agg([
            (pl.col("event_type") == "view").sum().alias("views"),
            (pl.col("event_type") == "cart").sum().alias("carts"),
            (pl.col("event_type") == "purchase").sum().alias("purchases"),
        ])
    )
    print(result)
```

### Pandas

```python
def filtering_counting():
    df = pd.read_csv(dataset_path)
    purchases = df[df["event_type"] == "purchase"]
    print(purchases)
    print("Count:", len(purchases))

def filtering_grouping_aggregation():
    df = pd.read_csv(dataset_path)
    result = (
        df[df["event_type"] == "purchase"]
        .groupby("category_code")["price"]
        .sum()
        .reset_index(name="total_sales")
    )
    print(result)

def grouping_conditional_aggregation():
    df = pd.read_csv(dataset_path)
    result = (
        df.groupby("category_code")["event_type"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    print(result)
```

---

## Visualization

After benchmarking, results are exported as CSV files and visualized. Plots can compare a single backend, two backends (e.g., DuckDB & Polars), or all three backends. (Insert your plot examples here.)

---

This modular design allows flexible benchmarking and comparison of different data processing backends, supporting both “hot” (cached) and “cold” (uncached) runs, with clear output and extensible OLAP implementations.

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*