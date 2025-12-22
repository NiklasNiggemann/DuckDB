from typing import List
from datetime import date
import polars as pl
from memory_profiler import profile

def run_polars_query(parquet_files: List[str]) -> int:
    df = (
        pl.scan_parquet(parquet_files)
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1)) &
            (pl.col("l_shipdate") <  date(1995, 1, 1)) &
            (pl.col("l_discount").is_between(0.05, 0.07)) &
            (pl.col("l_quantity") < 24)
        )
        .select(pl.len().alias("cnt"))
        .collect(engine="streaming")
    )
    print(df['cnt'][0])
    return df['cnt'][0]

@profile
def normal_test(scale_factor: int):
    parquet_path = f"tpc/lineitem_{scale_factor}.parquet"
    run_polars_query([parquet_path])

@profile
def stress_test(scale_factors: List[int]):
    parquet_files = [f"tpc/lineitem_{sf}.parquet" for sf in scale_factors]
    run_polars_query(parquet_files)

@profile
def multiple_test(num_files: int):
    parquet_files = [f"tpc/lineitem_10.parquet"] * num_files
    run_polars_query(parquet_files)
