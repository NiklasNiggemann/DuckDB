import duckdb
import utils
import subprocess

def main():

    query_cloud(first_time_setup())
    # query_dual(first_time_setup())
    create_query_graph()

def first_time_setup():
    con = duckdb.connect('md:')
    con.sql("CREATE DATABASE tpch")
    con.sql("USE tpch")
    con.sql("CALL dbgen(sf=0.01)")
    con.sql("PRAGMA enable_profiling='json'")
    con.sql("PRAGMA profiling_output='graph.json'")
    con.sql("PRAGMA profiling_coverage='ALL'")
    con.sql("PRAGMA profiling_mode='detailed'")
    return con

def query_cloud(con):
    result = con.sql("EXPLAIN SELECT n_name, COUNT(*) FROM customer, nation WHERE c_nationkey = n_nationkey GROUP BY n_name").fetchall()

def query_dual(con):
    con.sql("COPY customer TO './customer.parquet'")
    con.sql("EXPLAIN SELECT n_name, COUNT(*) FROM './customer.parquet', nation WHERE c_nationkey = n_nationkey GROUP BY n_name").fetchall()

def create_query_graph():
    command = ["python", "-m", "duckdb.query_graph", "graph.json"]
    subprocess.run(command)

main()