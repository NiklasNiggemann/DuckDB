# DuckDB vs Polars vs Pandas (vs PyArrow)

## Overview

This project benchmarks the performance of popular DataFrame and analytical query engines—**DuckDB**, **Polars**, and **Pandas**—by comparing their execution time and memory usage on a set of fundamental OLAP operations. These operations are the backbone of analytical queries in data warehousing and business intelligence:

- **Filtering**
- **Aggregation**
- **Grouping**
- **Counting**
- **Conditional Aggregation**

The [dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) used for this benchmark captures customer behavior in an online store. It is substantial in size, weighing in at 9 GB with 67,501,979 rows and 18 columns.

## Key Observations

A notable secondary finding was the significant variability in both execution time and memory usage for Pandas and Polars, contrasted with the remarkable stability of DuckDB—especially in terms of memory consumption. For example, memory usage for Polars and Pandas fluctuated by thousands of megabytes between runs, while DuckDB’s usage varied only by tenths of a megabyte. This consistency in DuckDB’s performance inspired a dedicated benchmarking project to further investigate its underlying technological advantages.

## Future Work

Looking ahead, the project aims to incorporate **PyArrow**, particularly to explore its strengths in enabling interoperability between different file formats and analytical engines.
