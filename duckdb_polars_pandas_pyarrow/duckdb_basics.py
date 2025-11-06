import duckdb
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def purchases_and_count():
    duckdb.sql(f"SELECT * FROM read_csv_auto('{dataset_path}') WHERE event_type= 'purchase'").show()
    duckdb.sql(f"SELECT COUNT(*) FROM read_csv_auto('{dataset_path}') WHERE event_type= 'purchase'").show()

def purchases_samsung_and_count():
    duckdb.sql(f"SELECT * FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase' AND category_code = 'electronics.smartphone' AND brand = 'samsung'").show()
    duckdb.sql(f"SELECT COUNT(*) FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase' AND category_code = 'electronics.smartphone' AND brand = 'samsung'").show()

def total_sales_per_category():
    duckdb.sql(f"SELECT category_code, SUM(price) AS total_sales FROM read_csv_auto('{dataset_path}') WHERE event_type='purchase' GROUP BY category_code").show()

def total_sales_per_category_by_brand():
    duckdb.sql(f"SELECT category_code, brand, SUM(price) AS total_sales FROM read_csv_auto('{dataset_path}') WHERE event_type='purchase' GROUP BY category_code, brand").show()

def purchases_per_event_by_category():
    duckdb.sql(f"SELECT category_code, SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views, SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases FROM read_csv_auto('{dataset_path}')) GROUP BY category_code").show()
