import pandas as pd
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def filtering_counting():
    df = pd.read_csv(dataset_path)
    purchases = df[df["event_type"] == "purchase"]
    print(purchases)
    print("Count:", len(purchases))


def filtering_grouping_aggregation():
    df = pd.read_csv(dataset_path)
    result = (
        df[df["event_type"] == "purchase"]
        .groupby("category_code")["price"]
        .sum()
        .reset_index(name="total_sales")
    )
    print(result)


def grouping_conditional_aggregation():
    df = pd.read_csv(dataset_path)
    result = (
        df.groupby("category_code")["event_type"]
        .value_counts()
        .unstack(fill_value=0)
        .reset_index()
    )
    print(result)
