import os
from typing import List

import pandas as pd
from memory_profiler import profile
import duckdb

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

def normal_test_chunked(scale_factor: int):
    con = duckdb.connect()
    parquet_path = f"tpc/lineitem_{scale_factor}.parquet"
    con.execute(f"PRAGMA threads={os.cpu_count()}")
    processed_count = 0
    chunk_size = 100000
    count = 0

    while True:
        # Read a chunk
        chunk = con.execute(
            f"SELECT * FROM '{parquet_path}' LIMIT {chunk_size} OFFSET {processed_count}"
        ).fetchdf()
        if len(chunk) == 0:
            break

        # Apply the filter to the chunk using pandas
        filtered = chunk[
            (chunk['l_shipdate'] >= '1994-01-01') &
            (chunk['l_shipdate'] < '1995-01-01') &
            (chunk['l_discount'] >= 0.05) &
            (chunk['l_discount'] <= 0.07) &
            (chunk['l_quantity'] < 24)
        ]
        count += len(filtered)
        processed_count += len(chunk)

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
    print(con.sql(query).pl())
    con.close()

