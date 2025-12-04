import pandas as pd
import utils
from memory_profiler import profile

dataset_path = f"{utils.get_dataset_dir()}/eCommerce.csv"

#@profile
#def total_sales_per_region():