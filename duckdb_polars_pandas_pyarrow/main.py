import utils
import duckdb_basics
import polars_basics
import pandas_basics
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

@profile
def main(selected_function):
    with utils.Timer():
        selected_function()

main(duckdb_basics.purchases_and_count)
main(polars_basics.purchases_and_count)
main(pandas_basics.purchases_and_count)