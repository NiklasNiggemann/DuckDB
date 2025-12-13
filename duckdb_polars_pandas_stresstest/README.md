# DuckDB vs. Polars -- Stress-Test 

This benchmark evaluates the performance and scalability of DuckDB and Polars for analytical workloads using the TPC-H lineitem table as a representative dataset. Parquet files were generated at multiple TPC-H scale factors, ranging from 1 to 640, with the largest table reaching 140 GB. The primary query applies a set of filters and counts the resulting rows, measuring both execution time and peak memory usage.

To stress-test the engines, a UNION ALL query is constructed by incrementally combining multiple large lineitem tables (each 140 GB) in a single query, simulating scenarios where analytical engines must process several large partitions or tables concurrently. This approach ensures that the engines must read and process the full data volume of all input tables, regardless of whether the tables contain identical or distinct data, as UNION ALL simply concatenates all rows without deduplication.

The benchmark is designed to reveal differences in I/O throughput, memory management, and query planning between DuckDB and Polars under increasing data and workload complexity. All experiments are conducted under controlled hardware and software environments to ensure fairness and reproducibility.

## Contributing

Pull requests and issues are welcome!
