from statistics import pvariance
import pyarrow.csv as pv
import pyarrow.compute as pc
import pyarrow as pa
import utils

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

def _read_table():
    return pv.read_csv(dataset_path).to_table()

def purchases_and_count():
    table = _read_table()
    mask = pc.equal(table["event_type"], "purchase")
    purchases = table.filter(mask)
    print(purchases)
    print("Count:", purchases.num_rows)

def purchases_samsung_and_count():
    table = _read_table()
    mask = (
        pc.and_(
            pc.equal(table["event_type"], "purchase"),
            pc.equal(table["category_code"], "electronics.smartphone"),
            pc.equal(table["brand"], "samsung"),
        )
    )
    samsung = table.filter(mask)
    print(samsung)
    print("Count:", samsung.num_rows)

def total_sales_per_category():
    table = _read_table()
    mask = pc.equal(table["event_type"], "purchase")
    filtered = table.filter(mask)
    group = pc.group_by(
        [filtered["category_code"]],
        aggregates=[("sum", filtered["price"])]
    )
    print(group)

def total_sales_per_category_by_brand():
    table = _read_table()
    mask = pc.equal(table["event_type"], "purchase")
    filtered = table.filter(mask)
    group = pc.group_by(
        [filtered["category_code"], filtered["brand"]],
        aggregates=[("sum", filtered["price"])]
    )
    print(group)

def purchases_per_event_by_category():
    table = _read_table()
    # Count views, carts, purchases per category_code
    categories = table["category_code"].unique()
    result = []
    for cat in categories:
        mask = pc.equal(table["category_code"], cat.as_py())
        sub = table.filter(mask)
        views = pc.sum(pc.equal(sub["event_type"], "view")).as_py()
        carts = pc.sum(pc.equal(sub["event_type"], "cart")).as_py()
        purchases = pc.sum(pc.equal(sub["event_type"], "purchase")).as_py()
        result.append({
            "category_code": cat.as_py(),
            "views": views,
            "carts": carts,
            "purchases": purchases,
        })
    import pandas as pd
    print(pd.DataFrame(result))