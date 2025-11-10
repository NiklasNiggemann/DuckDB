# DuckDB vs SQLite

This project benchmarks **DuckDB** against **SQLite** for analytical workloads using a large, real-world dataset: [Ecommerce Behavior Data from Multi-category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store).

Both DuckDB and SQLite are embedded, in-process, SQL-based database engines. While SQLite is primarily designed for OLTP (transactional) workloads and is widely used for local storage and lightweight applications, DuckDB is optimized for OLAP (analytical) queries and excels at handling large-scale data analysis.

### Dataset Details

- **Size:** 9 GB
- **Rows:** 67,501,979
- **Columns:** 9

This dataset size is representative of production-scale analytics scenarios.

### Why Compare DuckDB and SQLite?

Although comparing these engines in an OLAP setting may seem unfair—since SQLite is not intended for analytical workloads—SQLite remains a popular choice for local data exploration due to its simplicity and ubiquity. This project demonstrates the practical differences in performance and usability between the two engines when handling large analytical queries.

### Key Findings

- **Performance:** DuckDB completes analytical queries in seconds, while SQLite requires several minutes for the same tasks – even if it only needs to print the first 5 rows. 
- **Simplicity:** DuckDB queries can be executed in just 4 lines of code, whereas SQLite requires more than double that, even with the help of pandas.
- **Suitability:** DuckDB is purpose-built for analytics, while SQLite is best suited for transactional workloads and smaller datasets.

### Further Reading

For a deeper technical comparison, see:  
[VLDB: DuckDB vs SQLite](https://www.vldb.org/pvldb/vol15/p3535-gaffney.pdf)
