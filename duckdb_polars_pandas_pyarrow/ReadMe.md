# DuckDB vs Polars vs Pandas (& PyArrow)

## Overview

This project benchmarks the performance of popular DataFrame and analytical query engines—**DuckDB**, **Polars**, and **Pandas**. DuckDB is an in-process SQL OLAP database, similar to SQLite but optimized for analytics. Pandas DataFrame is the most popular tabular data structure in Python, provided by the Pandas library. And Polars DataFrames is a newer, high-performance DataFrame library for Python (and Rust).
It compares the execution time and memory usage on a set of fundamental OLAP operations. These operations are the backbone of analytical queries in data warehousing and business intelligence:

- **Filtering**
- **Aggregation**
- **Grouping**
- **Counting**
- **Conditional Aggregation**

The [dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) used for this benchmark captures customer behavior in an online store. It is substantial in size, weighing in at 9 GB with 67,501,979 rows and 18 columns.

## Benchmarking

Benchmarking was done with the provided setup in the code – it utilizes [subprocesses](https://docs.python.org/3/library/subprocess.html) to prevent warm-benchmarks, as the used ram etc. gets significantly smaller with each run. With this setup, each run is cold, so results are not distorted. The displayed values below are the calculated median of 10 runs & the min/max values. 
The tests were run on an MacBook Pro (2021) with an M1 Max Chip and 32 GB Ram. 

Certainly! Here’s a **well-formatted and elaborated findings section** suitable for a GitLab repository, using Markdown for clarity and professionalism. Each benchmark is explained, and the results are presented in tables for easy comparison.

# Benchmark Results: DataFrame Libraries

This document summarizes the performance benchmarks for three popular DataFrame libraries: **DuckDB**, **Polars**, and **Pandas**. The benchmarks cover three common data processing tasks:

1. **Filtering & Counting**
2. **Filtering, Grouping & Aggregation**
3. **Grouping & Conditional Aggregation**

For each task, we report execution time (in seconds) and memory usage (in MB). The metrics include median, minimum, maximum, and span (max - min) values, based on repeated runs.

## 1. Filtering & Counting (`purchases_and_count`)

This benchmark measures the time and memory required to filter a dataset and count the resulting rows.

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.72            | 2.62         | 2.78         | 0.16          | 443.45             | 322.50          | 565.66          | 234.16           |
| Polars  | 3.59            | 2.98         | 6.10         | 3.12          | 10,470.15          | 8,975.81        | 11,437.34       | 2,461.53         |
| Pandas  | 59.53           | 58.64        | 61.05        | 2.41          | 10,696.31          | 9,955.73        | 10,859.45       | 903.72           |

**Elaboration:**  
DuckDB demonstrates the fastest and most memory-efficient performance for simple filtering and counting. Polars is slightly slower and uses significantly more memory, while Pandas is much slower and also memory-intensive. DuckDB’s low memory footprint is especially notable.

## 2. Filtering, Grouping & Aggregation (`total_sales_per_category`)

This benchmark evaluates the performance of filtering data, grouping by a category, and aggregating sales totals.

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.33            | 2.28         | 2.43         | 0.15          | 241.85             | 240.66          | 363.50          | 122.84           |
| Polars  | 4.21            | 3.76         | 5.89         | 2.13          | 10,071.15          | 7,994.36        | 10,725.25       | 2,730.89         |
| Pandas  | 61.28           | 59.60        | 63.13        | 3.53          | 10,167.39          | 9,385.39        | 10,438.36       | 1,052.97         |

**Elaboration:**  
DuckDB again leads in both speed and memory efficiency, completing the task in under 2.5 seconds with minimal memory usage. Polars is faster than Pandas but still uses much more memory than DuckDB. Pandas remains the slowest and is also memory-hungry.

## 3. Grouping & Conditional Aggregation (`purchases_per_event_by_category`)

This benchmark tests grouping by event and category, with conditional aggregation.

| Library | Median Time (s) | Min Time (s) | Max Time (s) | Span Time (s) | Median Memory (MB) | Min Memory (MB) | Max Memory (MB) | Span Memory (MB) |
|---------|-----------------|--------------|--------------|---------------|--------------------|-----------------|-----------------|------------------|
| DuckDB  | 2.38            | 2.35         | 2.41         | 0.06          | 180.24             | 179.31          | 210.88          | 31.57            |
| Polars  | 11.73           | 8.49         | 14.15        | 5.66          | 6,308.01           | 5,684.81        | 7,499.42        | 1,814.61         |
| Pandas  | 61.75           | 61.61        | 62.98        | 1.37          | 9,840.26           | 9,329.61        | 10,243.22       | 913.61           |

**Elaboration:**  
DuckDB’s performance is outstanding, with both the lowest execution time and memory usage. Polars’ execution time increases for this more complex operation, and memory usage remains high. Pandas is again the slowest, with high memory consumption.

## **Summary & Recommendations**

- **DuckDB** consistently outperforms both Polars and Pandas in terms of speed and memory efficiency across all tested operations. It is especially well-suited for large-scale data processing tasks where resource usage is a concern.
- **Polars** offers better performance than Pandas but at the cost of significantly higher memory usage compared to DuckDB. It may be preferable when working with columnar data and when memory is not a limiting factor.
- **Pandas** is the slowest and most memory-intensive of the three, making it less suitable for large datasets or performance-critical applications.

**Recommendation:**  
For high-performance data processing in Python, especially with large datasets, **DuckDB** is the preferred choice based on these benchmarks.

## Future Work

Looking ahead, the project aims to incorporate **PyArrow**, particularly to explore its strengths in enabling interoperability between different file formats and analytical engines.
