import pandas as pd
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def purchases_and_count():
    df = pd.read_csv(dataset_path)
    purchases = df[df["event_type"] == "purchase"]
    print(purchases)
    print("Count:", len(purchases))

def purchases_samsung_and_count():
    df = pd.read_csv(dataset_path)
    samsung = df[
        (df["event_type"] == "purchase") &
        (df["category_code"] == "electronics.smartphone") &
        (df["brand"] == "samsung")
    ]
    print(samsung)
    print("Count:", len(samsung))

def total_sales_per_category():
    df = pd.read_csv(dataset_path)
    result = (
        df[df["event_type"] == "purchase"]
        .groupby("category_code")["price"]
        .sum()
        .reset_index(name="total_sales")
    )
    print(result)

def total_sales_per_category_by_brand():
    df = pd.read_csv(dataset_path)
    result = (
        df[df["event_type"] == "purchase"]
        .groupby(["category_code", "brand"])["price"]
        .sum()
        .reset_index(name="total_sales")
    )
    print(result)

def purchases_per_event_by_category():
    df = pd.read_csv(dataset_path)
    result = (
        df.groupby("category_code")["event_type"]
        .value_counts()
        .unstack(fill_value=0)
        .rename_axis(None, axis=1)
        .reset_index()
    )
    print(result)