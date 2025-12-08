import polars as pl
import utils
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

@profile
def filtering_counting():
    lf = pl.scan_csv(dataset_path)
    result = lf.filter(pl.col("event_type") == "purchase").select(pl.len()).collect(streaming=True)
    print(result)
