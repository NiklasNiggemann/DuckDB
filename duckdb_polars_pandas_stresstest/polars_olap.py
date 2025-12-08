import polars as pl
from datetime import date
from memory_profiler import profile

@profile
def pricing_summary_report(scale_factor: int):

    var1 = date(1998, 9, 2)
    parquet_path = f'tpc/lineitem_{scale_factor}.parquet'
    lineitem = pl.scan_parquet(parquet_path)
    ((((lineitem.filter(pl.col("l_shipdate") <= var1)
    .group_by("l_returnflag", "l_linestatus"))
    .agg(
        pl.sum("l_quantity").alias("sum_qty"),
        pl.sum("l_extendedprice").alias("sum_base_price"),
        (pl.col("l_extendedprice") * (1.0 - pl.col("l_discount")))
        .sum()
        .alias("sum_disc_price"),
        (
            pl.col("l_extendedprice")
            * (1.0 - pl.col("l_discount"))
            * (1.0 + pl.col("l_tax"))
        )
        .sum()
        .alias("sum_charge"),
        pl.mean("l_quantity").alias("avg_qty"),
        pl.mean("l_extendedprice").alias("avg_price"),
        pl.mean("l_discount").alias("avg_disc"),
        pl.len().alias("count_order"),
    ))
    .sort("l_returnflag", "l_linestatus"))
    .collect(streaming=True))

    print(lineitem)
