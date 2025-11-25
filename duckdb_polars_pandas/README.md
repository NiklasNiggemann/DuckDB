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
- **Warm:** All runs are executed in the same process, measuring performance with potential caching effects. Additionally, the script is executed 5 times before the measurement, to minimize noise and ensure detailed portraile of the memory efficiency of repeated execution with the help of caching.

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

# Warm run, 10 times with 5 warmups, Polars, filtering_grouping_aggregation
python benchmark.py --backend polars --function filtering_grouping_aggregation --mode warm 

```

**Available options:**
- `--backend`: `duckdb`, `polars`, or `pandas`
- `--function`:  
    - `filtering_counting`  
    - `filtering_grouping_aggregation`  
    - `grouping_conditional_aggregation`
- `--mode`: `cold` or `warm`
- `--runs`: Number of measured runs (default: 10)

Additionally, you can run comparative benchmarks across multiple backends:

- `--comparison`: `full`, `duckdb_polars`

  - `full`: Runs the selected function and mode on all backends sequentially.
  - `duckdb_polars`: Runs only DuckDB and Polars.

```sh
# Cold run, 10 times, comparing DuckDB, Polars, and Pandas
python benchmark.py --comparison full --function filtering_counting --mode cold --runs 10

# Warm run, 10 times with 5 warmups, comparing DuckDB and Polars
python benchmark.py --comparison duckdb_polars --function filtering_grouping_aggregation --mode warm --runs 10
```

---

## Code Explanation

The main module of this project is `benchmark.py`. This script orchestrates the benchmarking of different data processing tools (DuckDB, Polars, Pandas) via a command-line interface (CLI).

### Argument Parsing

First, we define the CLI arguments using Python’s `argparse` module:

```python
parser = argparse.ArgumentParser(description="Benchmark runner for data processing backends.")
parser.add_argument("--comparison", choices=["full", "duckdb_polars", None], default=None)
parser.add_argument("--tool", choices=["duckdb", "polars", "pandas"])
parser.add_argument("--function", choices=[
   "filtering_counting",
   "filtering_grouping_aggregation",
   "grouping_conditional_aggregation"
], required=True)
parser.add_argument("--mode", choices=["cold", "hot"], default="cold")
parser.add_argument("--runs", type=int, default=10)
```

### Benchmark Execution Logic

XXX 

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

After benchmarking, results are exported as CSV files and visualized. Plots can compare a single backend, two backends (e.g., DuckDB & Polars), or all three backends.

---

## Results 

### Filtering & Counting 

#### Cold Mode

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_counting_cold.png?ref_type=heads)

#### Hot Mode 

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot_bar.png?ref_type=heads) 
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_counting_hot.png?ref_type=heads)

### Filtering, Grouping & Aggregation 

#### Cold Mode 

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_grouping_aggregation_cold.png?ref_type=heads)

#### Hot Mode 

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_filtering_grouping_aggregation_hot.png?ref_type=heads)

### Grouping & Conditional Aggregation 

#### Cold Mode

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_cold.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_grouping_conditional_aggregation_cold.png?ref_type=heads)

#### Hot Mode 

![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot_cold_bar.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot.png?ref_type=heads)
![](https://gitlab.codecentric.de/data_ml_ai/duckdb-motherduck-lab/-/raw/main/duckdb_polars_pandas/results/duckdb_polars_grouping_conditional_aggregation_hot.png?ref_type=heads)

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*