import duckdb
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def filtering_and_counting():
    """
    Filters for purchase events and prints a sample of the filtered rows and their count.
    """
    duckdb.sql(
        f"SELECT * FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()
    duckdb.sql(
        f"SELECT COUNT(*) AS purchase_count FROM read_csv_auto('{dataset_path}') WHERE event_type = 'purchase'"
    ).show()


def filtering_grouping_aggregation():
    """
    Filters for purchase events, groups by category, and sums total sales.
    """
    duckdb.sql(
        f"""
        SELECT category_code, SUM(price) AS total_sales
        FROM read_csv_auto('{dataset_path}')
        WHERE event_type = 'purchase'
        GROUP BY category_code
        ORDER BY total_sales DESC
        """
    ).show()


def grouping_and_conditional_aggregation():
    """
    Groups by category and counts views, carts, and purchases for each category.
    """
    duckdb.sql(
        f"""
        SELECT
            category_code,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases
        FROM read_csv_auto('{dataset_path}')
        GROUP BY category_code
        ORDER BY purchases DESC
        """
    ).show()
