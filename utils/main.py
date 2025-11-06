import duckdb
import pandas as pd
import polars as pl
import pyarrow as pa
import utils

duckdb.sql(f"SELECT * FROM read_csv_auto('{utils.get_dataset_dir()}/netflix.csv')").show()