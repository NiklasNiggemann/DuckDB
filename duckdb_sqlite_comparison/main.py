import os
import duckdb
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
shared_resources = os.path.join(current_dir, '..', 'shared_resources')
shared_resources = os.path.normpath(shared_resources)

class Timer:
    def __enter__(self):
        self._enter_time = time.time()

    def __exit__(self, *exc_args):
        self._exit_time = time.time()
        print(f"{self._exit_time - self._enter_time:.2f} seconds elapsed")

with Timer():
    duckdb.sql(f"SELECT * FROM read_csv_auto('{shared_resources}/netflix_titles.csv')").show()

with Timer():
    duckdb.sql(f"SELECT count(*), type FROM read_csv_auto('{shared_resources}/netflix_titles.csv') GROUP BY type").show()
