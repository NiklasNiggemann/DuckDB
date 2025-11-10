import sqlite3
import pandas as pd
import utils

con = sqlite3.connect(":memory:")
cur = con.cursor()
print("Database created")

df = pd.read_csv(f"{utils.get_dataset_dir()}/ecommerce.csv")
print("DataFrame created")

df.to_sql("ecommerce", con, if_exists='replace', index=False)
print("DataFrame converted to Table")

res = cur.execute("SELECT * FROM ecommerce")
rows = res.fetchall()
print("SQL query executed")

for row in rows[:5]:
    print(row)
