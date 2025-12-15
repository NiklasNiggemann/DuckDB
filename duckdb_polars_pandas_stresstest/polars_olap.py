from datetime import date
from typing import List
import polars as pl
from memory_profiler import profile

@profile
def normal_test(scale_factor: int):
    # Use lazy reading for efficiency
    df = (
        pl.scan_parquet(f"tpc/lineitem_{scale_factor}.parquet")
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1)) &
            (pl.col("l_shipdate") <  date(1995, 1, 1)) &
            (pl.col("l_discount").is_between(0.05, 0.07)) &
            (pl.col("l_quantity") < 24)
        )
        .select(pl.len().alias("count"))
        .collect(engine="streaming")
    )
    print(df)

@profile
def stress_test(scale_factors: List[int]):
    # Create a list of lazy frames, then concatenate
    lazy_frames = [
        pl.scan_parquet(f"tpc/lineitem_{sf}.parquet") for sf in scale_factors
    ]
    df = (
        pl.concat(lazy_frames)
        .filter(
            (pl.col("l_shipdate") >= date(1994, 1, 1)) &
            (pl.col("l_shipdate") <  date(1995, 1, 1)) &
            (pl.col("l_discount").is_between(0.05, 0.07)) &
            (pl.col("l_quantity") < 24)
        )
        .select(pl.len().alias("cnt"))
        .collect(engine="streaming")
    )
    print(df)
