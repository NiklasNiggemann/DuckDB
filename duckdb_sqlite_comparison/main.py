import duckdb_basics
import utils

def main(selected_func):
    with utils.Timer():
        selected_func()

main(duckdb_basics.purchases_and_count)
