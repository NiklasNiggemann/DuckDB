import duckdb
import time
import os

current_dir = os.path.dirname(os.path.abspath(__file__))
datasets = os.path.join(current_dir, '..', 'datasets')
datasets = os.path.normpath(datasets)

class Timer:
    def __enter__(self):
        self._enter_time = time.time()

    def __exit__(self, *exc_args):
        self._exit_time = time.time()
        print(f"{self._exit_time - self._enter_time:.2f} seconds elapsed")

def main(selected_func):
    with Timer():
        selected_func()

def purchases_and_count():
    duckdb.sql(f"SELECT * FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type= 'purchase'").show()
    duckdb.sql(f"SELECT COUNT(*) FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type= 'purchase'").show()

def purchases_samsung_and_count():
    duckdb.sql(f"SELECT * FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type = 'purchase' AND category_code = 'electronics.smartphone' AND brand = 'samsung'").show()
    duckdb.sql(f"SELECT COUNT(*) FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type = 'purchase' AND category_code = 'electronics.smartphone' AND brand = 'samsung'").show()

def total_sales_per_category():
    duckdb.sql(f"SELECT category_code, SUM(price) AS total_sales FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type='purchase' GROUP BY category_code").show()

def total_sales_per_category_by_brand():
    duckdb.sql(f"SELECT category_code, brand, SUM(price) AS total_sales FROM read_csv_auto('{datasets}/eCommerce.csv') WHERE event_type='purchase' GROUP BY category_code, brand").show()

def purchases_per_event_by_category():
    duckdb.sql(f"SELECT category_code, SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views, SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases FROM read_csv_auto('{datasets}/eCommerce.csv') GROUP BY category_code").show()

main(purchases_and_count)
