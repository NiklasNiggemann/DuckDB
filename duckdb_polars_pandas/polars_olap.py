import polars as pl
import utils
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

@profile
def filtering_counting():
    lf = pl.scan_csv(dataset_path)
    result = lf.filter(pl.col("event_type") == "purchase").count().collect()
    print(result)

@profile
def filtering_grouping_aggregation():
    df = pl.read_csv(dataset_path)
    result = (
        df.filter(pl.col("event_type") == "purchase")
        .group_by("category_code")
        .agg(pl.col("price").sum().alias("total_sales"))
    )
    print(result)

@profile
def grouping_conditional_aggregation():
    df = pl.read_csv(dataset_path)
    result = (
        df.group_by("category_code")
        .agg([
            (pl.col("event_type") == "view").sum().alias("views"),
            (pl.col("event_type") == "cart").sum().alias("carts"),
            (pl.col("event_type") == "purchase").sum().alias("purchases"),
        ])
    )
    print(result)
