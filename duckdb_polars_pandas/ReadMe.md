# DuckDB vs Polars vs Pandas: A Modern DataFrame Benchmark

## Overview

This project benchmarks the performance of three leading DataFrame and analytical query engines in Python: **DuckDB**, **Polars**, and **Pandas**. Each tool represents a unique approach to tabular analytics:

- **DuckDB**: An in-process SQL OLAP database, similar to SQLite but optimized for analytical workloads.
- **Polars**: A modern, high-performance DataFrame library written in Rust, designed for speed and efficiency.
- **Pandas**: The most widely used DataFrame library in Python, known for its rich API and extensive ecosystem.

This benchmarking project was created to accompany a blog article for codecentric. For more background information and discussion of results, please refer to that article.

---

## Dataset

The benchmarks use the [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset, which is a real-world dataset with:

- **Size:** 9 GB
- **Rows:** 67,501,979
- **Columns:** 9

This scale is representative of production analytics scenarios.

---

## Benchmarking Methodology

Benchmarks are conducted using a robust, **modular, and fully CLI-driven** setup. The framework supports three benchmarking modes:

- **Cold:** Each run is executed in a new subprocess, eliminating caching effects and simulating a "cold start."
- **Hot:** All runs are executed in the same process, measuring performance with potential caching effects.

Each operation is executed multiple times (default: 10), and the median, minimum, maximum, and span (max - min), mean, standard deviation and Coefficient of variation are reported for both execution time (in seconds) and memory usage (in MB).

**Test Environment:**  
- MacBook Pro (2021), Apple M1 Max, 32 GB RAM  
- Python environment as specified in the repository

---

## Benchmarked Operations

Three fundamental OLAP operations are evaluated:

1. **Filtering & Counting**  
   Select rows based on a condition and count them.
2. **Filtering, Grouping & Aggregation**  
   Filter rows, group by a category, and aggregate a numeric column.
3. **Grouping & Conditional Aggregation**  
   Group by category and count views, carts, and purchases for each category.

---

## Setup

Clone the repository and install dependencies using the provided `requirements.txt`:

```sh
pip install -r requirements.txt
```

**Dataset Placement:**  
The dataset `.csv` file should be placed in a separate directory. This setup allows multiple projects to share the same dataset directory. In the project directory, you need to create a `results` directory where the `.csv` and `.png` files are stored. 

```
- duckdb_polars_pandas/
    └── results 
- dataset/
    └── ecommerce.csv
```

---

## Running Benchmarks

The benchmarking framework is fully parameterized via the command line. You can select the backend, operation, mode, and number of runs without editing any code.

### Example CLI Usage

```sh
# Cold run, 10 times, DuckDB, filtering_and_counting
python benchmark.py --backend duckdb --function filtering_counting --mode cold --runs 10

# Hot run, 5 times, Polars, filtering_grouping_aggregation
python benchmark.py --backend polars --function filtering_grouping_aggregation --mode hot --runs 5

```

**Available options:**
- `--backend`: `duckdb`, `polars`, or `pandas`
- `--function`:  
    - `filtering_counting`  
    - `filtering_grouping_aggregation`  
    - `grouping_conditional_aggregation`
- `--mode`: `cold` or `hot`
- `--runs`: Number of measured runs (default: 10)

Additionally, you can run comparative benchmarks across multiple backends:

- `--comparison`: `full`, `duckdb_polars`

  - `full`: Runs the selected function and mode on all backends sequentially.
  - `duckdb_polars`: Runs only DuckDB and Polars.

```sh
# Cold run, 10 times, comparing DuckDB, Polars, and Pandas
python benchmark.py --comparison full --function filtering_counting --mode cold --runs 10

# Hot run, 5 times, comparing DuckDB and Polars
python benchmark.py --comparison duckdb_polars --function filtering_grouping_aggregation --mode hot --runs 5
```

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
            mem, t = benchmark_engine.hot_benchmark(func)
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

## Results 

### Filtering & Counting 

#### Cold Mode

Elapsed Time (s)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 2.40 | 12.69 | 66.12 |
| **Std**  | 0.04 | 2.22  | 2.01  |
| **CV**   | 1.5% | 17.5% | 3.0%  |
| **Min**  | 2.35 | 7.36  | 62.66 |
| **Max**  | 2.46 | 15.79 | 70.59 |
| **Span** | 0.11 | 8.43  | 7.93  |

Memory Used (MB)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 29.45   | 682.19   | 272.35   |
| **Std**  | 66.27   | 1955.79  | 1854.91  |
| **CV**   | 225.0%  | 286.7%   | 681.1%   |
| **Min**  | 1.16    | -1391.91 | -2269.55 |
| **Max**  | 210.09  | 5504.22  | 3091.34  |
| **Span** | 208.94  | 6896.12  | 5360.89  |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_filtering_counting_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_counting_cold.png?ref_type=heads)

#### Hot Mode 

Elapsed Time (s)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 2.68 | 9.66  | 64.89 |
| **Std**  | 0.06 | 2.12  | 1.49  |
| **CV**   | 2.3% | 21.9% | 2.3%  |
| **Min**  | 2.62 | 5.82  | 63.42 |
| **Max**  | 2.79 | 13.28 | 68.13 |
| **Span** | 0.17 | 7.46  | 4.71  |

Memory Used (MB)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 40.25    | 633.19    | 65.54     |
| **Std**  | 87.52    | 2201.16   | 1750.45   |
| **CV**   | 217.4%   | 347.6%    | 2670.9%   |
| **Min**  | 0.11     | -1790.66  | -3198.94  |
| **Max**  | 260.69   | 5644.30   | 2808.44   |
| **Span** | 260.58   | 7434.95   | 6007.38   |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_filtering_counting_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_counting_hot.png?ref_type=heads)

### Filtering, Grouping & Aggregation 

#### Cold Mode 

Elapsed Time (s)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 2.31   | 2.85    | 59.53   |
| **Std**  | 0.05   | 0.05    | 0.35    |
| **CV**   | 2.0%   | 1.7%    | 0.6%    |
| **Min**  | 2.26   | 2.80    | 59.00   |
| **Max**  | 2.40   | 2.94    | 60.10   |
| **Span** | 0.14   | 0.14    | 1.10    |

Memory Used (MB)

| Metric | DuckDB   | Polars     | Pandas     |
|--------|----------|------------|------------|
| **Mean** | 211.28    | 12426.42   | 12267.93   |
| **Std**  | 20.91     | 0.63       | 980.08     |
| **CV**   | 9.9%      | 0.0%       | 8.0%       |
| **Min**  | 179.64    | 12425.55   | 11039.67   |
| **Max**  | 243.27    | 12427.61   | 14654.47   |
| **Span** | 63.63     | 2.06       | 3614.80    |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_filtering_grouping_aggregation_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_grouping_aggregation_cold.png?ref_type=heads)

#### Hot Mode 

Elapsed Time (s)

| Metric   | DuckDB | Polars | Pandas |
|----------|--------|--------|--------|
| **Mean** | 2.32   | 6.29   | 63.45  |
| **Std**  | 0.05   | 3.75   | 1.29   |
| **CV**   | 2.0%   | 59.6%  | 2.0%   |
| **Min**  | 2.25   | 3.13   | 61.99  |
| **Max**  | 2.38   | 16.08  | 66.36  |
| **Span** | 0.13   | 12.95  | 4.37   |

Memory Used (MB)

| Metric   | DuckDB | Polars   | Pandas   |
|----------|--------|----------|----------|
| **Mean** | 28.80  | 1164.60  | 34.07    |
| **Std**  | 65.23  | 3914.01  | 1412.61  |
| **CV**   | 226.5% | 336.1%   | 4146.5%  |
| **Min**  | 1.02   | -3156.58 | -2151.58 |
| **Max**  | 210.92 | 11123.97 | 2799.83  |
| **Span** | 209.91 | 14280.55 | 4951.41  |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_filtering_grouping_aggregation_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_grouping_aggregation_hot.png?ref_type=heads)

### Grouping & Conditional Aggregation 

#### Cold Mode

Elapsed Time (s)

| Metric   | DuckDB | Polars | Pandas |
|----------|--------|--------|--------|
| **Mean** | 2.42   | 9.75   | 61.62  |
| **Std**  | 0.06   | 0.79   | 0.83   |
| **CV**   | 2.5%   | 8.1%   | 1.3%   |
| **Min**  | 2.36   | 8.54   | 60.68  |
| **Max**  | 2.52   | 11.30  | 63.03  |
| **Span** | 0.16   | 2.76   | 2.35   |

Memory Used (MB)

| Metric   | DuckDB  | Polars   | Pandas    |
|----------|---------|----------|-----------|
| **Mean** | 219.92  | 8074.84  | 11574.46  |
| **Std**  | 40.84   | 849.14   | 548.85    |
| **CV**   | 18.6%   | 10.5%    | 4.7%      |
| **Min**  | 180.17  | 6093.03  | 11044.94  |
| **Max**  | 302.03  | 9177.55  | 12999.53  |
| **Span** | 121.86  | 3084.52  | 1954.59   |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_grouping_conditional_aggregation_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_grouping_conditional_aggregation_cold.png?ref_type=heads)

#### Hot Mode 

Elapsed Time (s)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 2.40 | 12.69 | 66.12 |
| **Std**  | 0.04 | 2.22  | 2.01  |
| **CV**   | 1.5% | 17.5% | 3.0%  |
| **Min**  | 2.35 | 7.36  | 62.66 |
| **Max**  | 2.46 | 15.79 | 70.59 |
| **Span** | 0.11 | 8.43  | 7.93  |

Memory Used (MB)

| Metric | DuckDB | Polars | Pandas |
|--------|--------|--------|--------|
| **Mean** | 29.45   | 682.19   | 272.35   |
| **Std**  | 66.27   | 1955.79  | 1854.91  |
| **CV**   | 225.0%  | 286.7%   | 681.1%   |
| **Min**  | 1.16    | -1391.91 | -2269.55 |
| **Max**  | 210.09  | 5504.22  | 3091.34  |
| **Span** | 208.94  | 6896.12  | 5360.89  |

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_pandas_grouping_conditional_aggregation_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_grouping_conditional_aggregation_hot.png?ref_type=heads)

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*