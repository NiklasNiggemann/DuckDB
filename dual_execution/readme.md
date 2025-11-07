# Dual Execution 

After loading data in an MotherDuck instance, it uses DuckDB in WASM to process data locally. This offers the possibilty for Dual Execution, where queries are intelligently tasked to either run locally or in the cloud. 

Using EXPLAIN, it is possible to see the physical plan of the query engine. 

https://www.youtube.com/watch?v=2eVNm4A8sLw