import duckdb
from typing import List
from memory_profiler import profile

def run_query(parquet_files: List[str]) -> int:
    files_str = ", ".join([f"'{f}'" for f in parquet_files])
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM read_parquet([{files_str}])
        WHERE l_shipdate >= DATE '1994-01-01'
          AND l_shipdate <  DATE '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24;
    """
    with duckdb.connect() as con:
        count = con.execute(query).fetchone()[0]
    print(count)
    return count

@profile
def normal_test(scale_factor: int):
    parquet_path = f"tpc/lineitem_{scale_factor}.parquet"
    run_query([parquet_path])

@profile
def stress_test(scale_factors: List[int]):
    parquet_files = [f"tpc/lineitem_{sf}.parquet" for sf in scale_factors]
    run_query(parquet_files)

