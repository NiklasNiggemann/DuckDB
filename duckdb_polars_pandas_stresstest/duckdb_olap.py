from typing import List

from memory_profiler import profile
import duckdb

@profile
def normal_test(scale_factor: int):
    con = duckdb.connect()
    parquet_path = f"tpc/lineitem_{scale_factor}.parquet"
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM '{parquet_path}'
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24;
    """
    print(con.sql(query).pl())

@profile
def stress_test(scale_factors: List[int]):
    con = duckdb.connect()
    union_query = " UNION ALL ".join([
        f"SELECT * FROM 'tpc/lineitem_{sf}.parquet'" for sf in scale_factors
    ])
    query = f"""
    SELECT COUNT(*) AS cnt
    FROM ({union_query}) AS all_lineitems
    WHERE l_shipdate >= DATE '1994-01-01'
      AND l_shipdate <  DATE '1995-01-01'
      AND l_discount BETWEEN 0.05 AND 0.07
      AND l_quantity < 24;
    """
    print(con.sql(query).pl())

