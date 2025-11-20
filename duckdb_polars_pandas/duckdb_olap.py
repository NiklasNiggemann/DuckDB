import duckdb
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def filtering_counting():
    duckdb.sql(
        f"SELECT * FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()
    duckdb.sql(
        f"SELECT COUNT(*) AS purchase_count FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()


def filtering_grouping_aggregation():
    duckdb.sql(
        f"""
        SELECT category_code, SUM(price) AS total_sales
        FROM read_csv_auto('{dataset_path}')
        WHERE event_type = 'purchase'
        GROUP BY category_code
        """
    ).show()


def grouping_conditional_aggregation():
    duckdb.sql(
        f"""
        SELECT
            category_code,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
        FROM read_csv_auto('{dataset_path}')
        GROUP BY category_code
        """
    ).show()
