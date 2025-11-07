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

Benchmarking was done with the provided setup in the code – it utilizes [subprocesses](https://docs.python.org/3/library/subprocess.html) to prevent warm-benchmarks, as the used ram etc. gets significantly smaller with each run. With this setup, each run is cold, so results are not distorted. 
The tests were run on an MacBook Pro (2021) with an M1 Max Chip and 32 GB Ram. 

1. Filterung & Counting (purchases_and_count) 

DuckDB: 
Polars: 
Pandas: 

2. Filterung, Grouping & Aggregation (total_sales_per_category)

DuckDB: 
Polars: 
Pandas: 

3. Grouping & Conditional Aggregation (purchases_per_event_by_category)

DuckDB: 
Polars: 
Pandas: 

## Future Work

Looking ahead, the project aims to incorporate **PyArrow**, particularly to explore its strengths in enabling interoperability between different file formats and analytical engines.
