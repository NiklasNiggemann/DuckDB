import os
from typing import List
import duckdb
from memory_profiler import profile

@profile
def normal_test(scale_factor: int):
    con = duckdb.connect()
    parquet_path = f"tpc/lineitem_{scale_factor}.parquet"
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM '{parquet_path}'
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24;
    """
    count = con.execute(query).fetchone()[0]
    print(count)
    con.close()

@profile
def stress_test(scale_factors: List[int]):
    con = duckdb.connect()
    union_query = " UNION ALL ".join([
        f"SELECT * FROM 'tpc/lineitem_{sf}.parquet'" for sf in scale_factors
    ])
    con.execute(f"SET max_expression_depth TO 10000")
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM ({union_query}) AS all_lineitems
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24;
    """
    count = con.execute(query).fetchone()[0]
    print(count)
    con.close()

@profile
def multiple_test(num_files: int):
    con = duckdb.connect()
    parquet_files = [f"tpc/lineitem_10.parquet"] * num_files
    files_str = ", ".join([f"'{f}'" for f in parquet_files])
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM read_parquet([{files_str}])
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24;
    """
    count = con.execute(query).fetchone()[0]
    print(count)
    con.close()
