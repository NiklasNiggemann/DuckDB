# DuckDB vs Polars vs Pandas (vs PyArrow)

## Idea 
This project takes the basic operations from DuckDB_Basics and compares their query execution time and memory usage with the common DataFrame tools Polars and Pandas.
The operations contain common OLAP workloads, which are the building blocks of analytical queries in data warehousing and business intelligence. 
- Filtering
- Aggregation
- Grouping
- Counting
- Conditional Aggregation

The used [dataset](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store) contains customer behavior in an online store and is rather big with 9 GB, containing 67,501,979 rows of data with 18 columns. 

### Implemented Functions 

## Benchmark Results

Each test for each tool was run 10 times with the setup in the code and the written result is the median of the measured values.
Run on a MacBook Pro (2021) with an M1 Max Chip and 32 GB Ram, initial testing of selected functions resulted in the following: 
1. Filtering & Counting [purchases_and_count()]
DuckDB – Median Time: 2.66 s – Time Span: 0.09 s – Median Memory: 17.44 MB – Max Memory: 290.86 MB – Min Memory: 0.16 MB – Memory Span: 323 MB 
Polars – 
Pandas –    
2. Filtering, Grouping & Aggregation [total_sales_per_category]
DuckDB – Median Time: 2.30 s – Time Span: 0.15 s – Median Memory: 1.89 MB – Max Memory: 241.42 MB – Min Memory: 0.67 MB – 323 MB 
Polars –   
Pandas –  
3. Grouping, Conditional Aggregation [purchases_per_event_by_category]
DuckDB – Median Time: 2.66 s – Time Span: 0.09 s – Median Memory: 17.44 MB – Memory Span: 323 MB 
Polars –  
Pandas –  

_Important_: This shown setup loops the process, resulting in a warm / hot cache performance, which is noticably faster than cold start performance – this is more representable for repeated queries in a production setting. A cold benchmark should be added in the future, as this would be representable of first-time queries & batch jobs.

**_Note_**: An interesting secondary effect was a noticable variety in the results for Pandas and Polars, and even more so the stability in the results of DuckDB. Especially the memory usage varieted in the thousands for Polars & Pandas, but only in the tenths for DuckDB. 

In the future, it is planned to also implement PyArrow, especially in regards its ability to enable connectivity between the file formats. 