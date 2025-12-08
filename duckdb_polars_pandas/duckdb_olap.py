import duckdb
import utils
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

@profile
def filtering_counting():
    duckdb.sql(f"SELECT COUNT(*) AS purchase_count FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'").show()
