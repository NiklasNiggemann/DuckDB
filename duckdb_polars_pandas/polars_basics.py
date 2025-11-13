import utils
import polars as pl

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def purchases_and_count():
    df = pl.read_csv(dataset_path)
    purchases = df.filter(pl.col("event_type") == "purchase")
    print(purchases)
    print("Count:", purchases.height)

def purchases_samsung_and_count():
    df = pl.read_csv(dataset_path)
    samsung = df.filter(
        (pl.col("event_type") == "purchase") &
        (pl.col("category_code") == "electronics.smartphone") &
        (pl.col("brand") == "samsung")
    )
    print(samsung)
    print("Count:", samsung.height)

def total_sales_per_category():
    df = pl.read_csv(dataset_path)
    result = (
        df.filter(pl.col("event_type") == "purchase")
        .group_by("category_code")
        .agg(pl.col("price").sum().alias("total_sales"))
    )
    print(result)

def total_sales_per_category_by_brand():
    df = pl.read_csv(dataset_path)
    result = (
        df.filter(pl.col("event_type") == "purchase")
        .group_by(["category_code", "brand"])
        .agg(pl.col("price").sum().alias("total_sales"))
    )
    print(result)

def purchases_per_event_by_category():
    df = pl.read_csv(dataset_path)
    result = (
        df.group_by("category_code")
        .agg([
            (pl.col("event_type") == "view").sum().alias("views"),
            (pl.col("event_type") == "cart").sum().alias("carts"),
            (pl.col("event_type") == "purchase").sum().alias("purchases"),
        ])
    )
    print(result)