import sys
import os
import duckdb
import polars as pl
import pandas as pd
import pyarrow as pa
import utils

duckdb.sql(f"SELECT * FROM read_csv_auto('{utils.get_dataset_dir()}/netflix.csv')").show()

