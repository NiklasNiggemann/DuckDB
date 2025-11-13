# DuckDB vs Polars vs Pandas: A Modern DataFrame Benchmark

## Abstract

This document benchmarks and compares the performance, design, and use cases of leading DataFrame and analytical query engines in Python: **DuckDB**, **Polars**, and **Pandas**. Each tool represents a distinct approach to tabular analytics, and this comparison aims to clarify their strengths, weaknesses, and best use cases.

---

## Introduction

Analytical workloads—such as filtering, grouping, and aggregation—are fundamental to data warehousing and business intelligence. The choice of engine can dramatically affect both performance and resource usage, especially as data volumes increase. This comparison provides a practical guide for selecting the right tool for your data processing needs.

---

## Background

### DataFrames

DataFrames are two-dimensional, tabular data structures with rows and columns, similar to spreadsheets or database tables. They are a fundamental data structure in libraries like Pandas and Polars, providing an intuitive way to store, view, and manipulate data.

---

## Overview of Libraries

### DuckDB

DuckDB is an in-process SQL OLAP database, similar to SQLite but optimized for analytical workloads. It is SQL-centric and built for those who work with SQL on a daily basis, focusing on simplicity and high performance for analytics and databases.

### Pandas

Pandas is an open-source library for Python, designed for data manipulation and analysis. It is the most widely used DataFrame library in Python, known for its rich API and extensive ecosystem. The DataFrame is its key data structure, offering many functions for cleaning, wrangling, and preparing data for analysis.

### Polars

Polars is a modern, high-performance DataFrame library written in Rust, designed for speed and efficiency. It leverages the Apache Arrow columnar memory format to achieve speed through parallel and lazy execution, query optimization, and efficient data handling. Polars is considered the new Pandas for data science and machine learning workflows.

---

## Benchmarking

### Purpose of Benchmarking

Performance is one of the easiest quality metrics to measure for any system. It is an attractive, "objective" measurement that makes it easy to compare different systems. However, fair benchmarking is challenging, and it is easy to misrepresent performance information, intentionally or not.

#### Common Mistakes in Benchmarking

1. **Non-Reproducibility:** All configuration parameters must be known and experiments must be reproducible.
2. **Failure to Optimize:** Systems should be properly optimized for the workload.
3. **Apples vs Oranges:** Ensure the same functionality is measured.
4. **Overly-specific Tuning:** Avoid tuning systems only for standardized benchmarks.
5. **Cold vs Hot Runs:** Report performance for both initial and subsequent runs.
6. **Cold vs Warm Runs:** Properly clear OS caches for cold run measurements.
7. **Ignoring Preprocessing Time:** Include setup and index creation time in benchmarks.
8. **Incorrect Code:** Always verify correctness of results.

### Benchmarking Methodology

Benchmarks are conducted using robust setups (e.g., Python subprocesses) to ensure each run is "cold"—eliminating caching effects and ensuring fair, repeatable results. Each operation is executed 10 times, and the median, minimum, maximum, and span are reported for both execution time and memory usage.

**Test Environment:**  
- MacBook Pro (2021), Apple M1 Max, 32 GB RAM  
- Python environment as specified in the repository

### Dataset

The [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) dataset is used:

- **Size:** 9 GB
- **Rows:** 67,501,979
- **Columns:** 18

---

## Benchmarked Operations

Three fundamental OLAP operations are evaluated:

1. **Filtering & Counting:** Select rows based on a condition and count them.
2. **Filtering, Grouping & Aggregation:** Filter rows, group by a category, and aggregate a numeric column.
3. **Grouping & Conditional Aggregation:** Group by multiple columns and perform conditional aggregations.

---

## Results

### 1. Filtering & Counting

| Library | Median Time (s) | Median Memory (MB) |
|---------|-----------------|--------------------|
| DuckDB  | 2.72            | 443.45             |
| Polars  | 3.59            | 10,470.15          |
| Pandas  | 59.53           | 10,696.31          |

**Analysis:**  
DuckDB is fastest and most memory-efficient. Polars is competitive in speed but uses much more memory. Pandas is slowest and most memory-intensive.

---

### 2. Filtering, Grouping & Aggregation

| Library | Median Time (s) | Median Memory (MB) |
|---------|-----------------|--------------------|
| DuckDB  | 2.33            | 241.85             |
| Polars  | 4.21            | 10,071.15          |
| Pandas  | 61.28           | 10,167.39          |

**Analysis:**  
DuckDB again outperforms both Polars and Pandas. Polars is faster than Pandas but still uses much more memory. Pandas is slowest and most memory-hungry.

---

### 3. Grouping & Conditional Aggregation

| Library | Median Time (s) | Median Memory (MB) |
|---------|-----------------|--------------------|
| DuckDB  | 2.38            | 180.24             |
| Polars  | 11.73           | 6,308.01           |
| Pandas  | 61.75           | 9,840.26           |

**Analysis:**  
DuckDB’s performance is exceptional, with both the lowest execution time and memory usage. Polars’ execution time increases for more complex operations, and memory usage remains high. Pandas is again the slowest.

---

## Discussion

### Pandas vs Polars

- **Polars** is designed for performance, written in Rust, and leverages safe concurrency and parallelism. It uses Apache Arrow for efficient, columnar, and interoperable data storage.
- **Pandas** is built on Python and NumPy, which limits its performance, especially with non-numeric data types and large datasets. It uses eager execution, while Polars supports both eager and lazy execution with query optimization.
- **API Expressiveness:** Polars has an expressive API, allowing most operations as built-in methods, while Pandas often requires custom functions with `.apply()`, which is slower.

### DuckDB vs Polars

- **DuckDB** is SQL-centric, simple, and optimized for analytics and databases.
- **Polars** is DataFrame-centric, high-performance, and aimed at ETL and pipeline tools. It is fast but can be more complex and require more code for the same functionality.
- **Overlap:** Both tools have overlapping use cases, but DuckDB is generally more accessible for SQL-heavy workflows, while Polars excels in DataFrame-centric, columnar operations.

### When to Use What?

- **Pandas** is best for small to medium datasets and when leveraging the extensive Python data science ecosystem.
- **Polars** is ideal for high-performance, columnar operations and can handle larger datasets efficiently if memory is available.
- **DuckDB** is the preferred choice for large-scale analytics, offering the best performance and memory efficiency.
- **Interoperability:** DuckDB, Polars, and Pandas can often be used together, depending on the workflow.

---

## DuckDB Integration

DuckDB integrates well with Polars and other Python tools. See [DuckDB Polars Integration Guide](https://duckdb.org/docs/stable/guides/python/polars) for more details.

---

## Conclusion

- **DuckDB** consistently delivers the fastest execution and lowest memory usage across all tested operations. Its SQL interface and in-process architecture make it ideal for large-scale analytics.
- **Polars** offers impressive speed and a modern API, but with higher memory usage.
- **Pandas** remains the most accessible and feature-rich for small to medium datasets but struggles at scale.

**Recommendation:**  
For high-performance, large-scale data processing in Python, **DuckDB** is the preferred choice. For those needing a modern DataFrame API and working with columnar data, **Polars** is a compelling alternative. **Pandas** is best reserved for smaller datasets or when its extensive ecosystem is required.

---

## Reproducibility

All benchmarking scripts, environment specifications, and raw results are available in this repository. Contributions and suggestions are welcome.

---

*For questions, improvements, or to contribute additional benchmarks, please open an issue or pull request.*