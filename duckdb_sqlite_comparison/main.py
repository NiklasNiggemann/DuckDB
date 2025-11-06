import os
import duckdb
import timer

current_dir = os.path.dirname(os.path.abspath(__file__))
shared_resources = os.path.join(current_dir, '..', 'shared_resources')
shared_resources = os.path.normpath(shared_resources)

duckdb.sql(f"SELECT * FROM read_csv_auto('{shared_resources}/netflix_titles.csv')").show()
duckdb.sql(f"SELECT count(*), type FROM read_csv_auto('{shared_resources}/netflix_titles.csv') GROUP BY type").show()
