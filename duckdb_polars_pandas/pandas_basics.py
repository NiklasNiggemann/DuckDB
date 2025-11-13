import pandas as pd
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def filtering_and_counting():
    """
    Filters for purchase events and prints a sample of the filtered DataFrame and its count.
    """
    df = pd.read_csv(dataset_path)
    purchases = df[df["event_type"] == "purchase"]
    print(purchases)
    print("Count:", len(purchases))


def filtering_grouping_aggregation():
    """
    Filters for purchase events, groups by category, and sums total sales.
    """
    df = pd.read_csv(dataset_path)
    result = (
        df[df["event_type"] == "purchase"]
        .groupby("category_code")["price"]
        .sum()
        .reset_index(name="total_sales")
    )
    print(result)


def grouping_and_conditional_aggregation():
    """
    Groups by category and counts views, carts, and purchases for each category.
    """
    df = pd.read_csv(dataset_path)
    result = (
        df.groupby("category_code")["event_type"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    print(result)
