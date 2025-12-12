import polars as pl
from datetime import date
from memory_profiler import profile

@profile
def normal_test(scale_factor: int):
    df = pl.SQLContext().execute(
        f"""
        SELECT COUNT(*) AS count
        FROM read_parquet('tpc/lineitem_{scale_factor}.parquet')
        WHERE l_shipdate >= '1994-01-01'
          AND l_shipdate <  '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
        """,
        eager=False
    ).collect(engine="streaming")
    print(df)

