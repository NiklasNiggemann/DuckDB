import pandas as pd
import utils
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

@profile
def filtering_counting():
    df = pd.read_csv(dataset_path)
    purchases = df[df["event_type"] == "purchase"]
    print("Count:", len(purchases))
