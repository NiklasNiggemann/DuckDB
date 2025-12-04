from datetime import datetime, timedelta

import polars as pl
import utils
from memory_profiler import profile

parquet_path = 'tpc/lineitem.parquet'

@profile
def total_sales_per_region(base_path: str = "tpc") -> pl.DataFrame:
    cutoff_date = datetime(1998, 12, 1) - timedelta(days=90)

    df = pl.read_parquet(parquet_path)

    result = (
        df
        .filter(pl.col("l_shipdate") <= pl.lit(cutoff_date))
        .groupby(["l_returnflag", "l_linestatus"])
        .agg([
            pl.col("l_quantity").sum().alias("sum_qty"),
            pl.col("l_extendedprice").sum().alias("sum_base_price"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount"))).sum().alias("sum_disc_price"),
            (pl.col("l_extendedprice") * (1 - pl.col("l_discount")) * (1 + pl.col("l_tax"))).sum().alias("sum_charge"),
            pl.col("l_quantity").mean().alias("avg_qty"),
            pl.col("l_extendedprice").mean().alias("avg_price"),
            pl.col("l_discount").mean().alias("avg_disc"),
            pl.count().alias("count_order"),
        ])
        .sort(["l_returnflag", "l_linestatus"])
    )

    print(result)

