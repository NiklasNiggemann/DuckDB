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

---

## Code Explanation



--- 

## Visualization

After benchmarking, results are exported as CSV files and visualized. Plots can compare a single backend, two backends (e.g., DuckDB & Polars), or all three backends.

---

## Results 

### Filtering & Counting 

#### Cold Mode

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_cold.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_cold_bar.png)

#### Hot Mode 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot_bar.png)

#### Comparison 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_counting_hot_cold_bar.png)

### Filtering, Grouping & Aggregation 

#### Cold Mode

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_cold.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_cold_bar.png)

#### Hot Mode 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot_bar.png)

#### Comparison 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_filtering_grouping_aggregation_hot_cold_bar.png)

### Grouping & Conditional Aggregation 

#### Cold Mode

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_cold.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_cold_bar.png)

#### Hot Mode 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot.png)
![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot_bar.png)

#### Comparison 

![](https://github.com/NiklasNiggemann/DuckDB/blob/main/duckdb_polars_pandas/results/pandas_duckdb_polars_grouping_conditional_aggregation_hot_cold_bar.png)

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*