# DuckDB vs Polars vs Pandas (and PyArrow): A Modern DataFrame Benchmark

## Overview

This project benchmarks the performance of leading DataFrame and analytical query engines in Python: **DuckDB**, **Polars**, and **Pandas**. Each tool represents a distinct approach to tabular analytics:

- **DuckDB**: An in-process SQL OLAP database, similar to SQLite but optimized for analytical workloads.
- **Polars**: A modern, high-performance DataFrame library written in Rust, designed for speed and efficiency.
- **Pandas**: The most widely used DataFrame library in Python, known for its rich API and extensive ecosystem.

Future benchmarks will also include **PyArrow**, which is increasingly used for efficient in-memory columnar data and interoperability between analytical engines.

### Purpose of Benchmarking

Analytical workloads—such as filtering, grouping, and aggregation—are fundamental to data warehousing and business intelligence. The choice of engine can dramatically affect both performance and resource usage, especially as data volumes increase.

### Dataset

The [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset is used for these benchmarks. It is a real-world dataset with:

- **Size:** 9 GB
- **Rows:** 67,501,979
- **Columns:** 18

This scale is representative of production analytics scenarios.

---

## Benchmarking Methodology

Benchmarks are conducted using a robust setup that leverages [Python subprocesses](https://docs.python.org/3/library/subprocess.html) to ensure each run is "cold"—eliminating caching effects and ensuring fair, repeatable results. Each operation is executed 10 times, and the median, minimum, maximum, and span (max - min) are reported for both execution time (in seconds) and memory usage (in MB).

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
   Group by multiple columns and perform conditional aggregations.

---

## Results

### 1. Filtering & Counting (`purchases_and_count`)

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.72            | 2.62         | 2.78         | 0.16          | 443.45             | 322.50          | 565.66          | 234.16           |
| Polars  | 3.59            | 2.98         | 6.10         | 3.12          | 10,470.15          | 8,975.81        | 11,437.34       | 2,461.53         |
| Pandas  | 59.53           | 58.64        | 61.05        | 2.41          | 10,696.31          | 9,955.73        | 10,859.45       | 903.72           |

**Analysis:**  
DuckDB provides both the fastest execution and the lowest memory usage for this operation. Polars is competitive in speed but uses significantly more memory. Pandas is much slower and more memory-intensive.

---

### 2. Filtering, Grouping & Aggregation (`total_sales_per_category`)

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.33            | 2.28         | 2.43         | 0.15          | 241.85             | 240.66          | 363.50          | 122.84           |
| Polars  | 4.21            | 3.76         | 5.89         | 2.13          | 10,071.15          | 7,994.36        | 10,725.25       | 2,730.89         |
| Pandas  | 61.28           | 59.60        | 63.13        | 3.53          | 10,167.39          | 9,385.39        | 10,438.36       | 1,052.97         |

**Analysis:**  
DuckDB again outperforms both Polars and Pandas, with sub-2.5 second execution and minimal memory usage. Polars is faster than Pandas but still uses much more memory than DuckDB. Pandas is the slowest and most memory-hungry.

---

### 3. Grouping & Conditional Aggregation (`purchases_per_event_by_category`)

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.38            | 2.35         | 2.41         | 0.06          | 180.24             | 179.31          | 210.88          | 31.57            |
| Polars  | 11.73           | 8.49         | 14.15        | 5.66          | 6,308.01           | 5,684.81        | 7,499.42        | 1,814.61         |
| Pandas  | 61.75           | 61.61        | 62.98        | 1.37          | 9,840.26           | 9,329.61        | 10,243.22       | 913.61           |

**Analysis:**  
DuckDB’s performance is exceptional, with both the lowest execution time and memory usage. Polars’ execution time increases for this more complex operation, and memory usage remains high. Pandas is again the slowest, with high memory consumption.

---

## Summary & Recommendations

### Key Takeaways

- **DuckDB** consistently delivers the fastest execution and lowest memory usage across all tested operations. Its SQL interface and in-process architecture make it ideal for large-scale analytics on modern hardware.
- **Polars** offers impressive speed, especially for simpler operations, but its memory usage is substantially higher than DuckDB’s. It is a strong choice for users who need fast columnar operations and can accommodate higher RAM usage.
- **Pandas** remains the most accessible and feature-rich for small to medium datasets, but it struggles with performance and memory efficiency at scale.

### Recommendation

For high-performance, large-scale data processing in Python, **DuckDB** is the preferred choice based on these benchmarks. For those needing a modern DataFrame API and working with columnar data, **Polars** is a compelling alternative. **Pandas** is best reserved for smaller datasets or when its extensive ecosystem is required.

---

## Future Work: PyArrow and Beyond

**PyArrow** is rapidly becoming the backbone of the Python data ecosystem, enabling zero-copy data interchange between libraries and supporting efficient columnar storage. Future benchmarks will:

- Evaluate PyArrow’s performance for similar OLAP operations
- Explore interoperability scenarios (e.g., DuckDB + PyArrow, Polars + PyArrow)
- Benchmark file format read/write speeds (Parquet, Feather, Arrow IPC)

---

## Reproducibility

All benchmarking scripts, environment specifications, and raw results are available in this repository. Contributions and suggestions are welcome.

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*
