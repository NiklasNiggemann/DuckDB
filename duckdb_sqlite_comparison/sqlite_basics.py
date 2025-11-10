import csv
import sqlite3

import utils

con = sqlite3.connect(":memory:")
cur = con.cursor()
cur.execute("CREATE TABLE ecommerce (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session);")

with open(f'{utils.get_dataset_dir()}/ecommerce.csv') as csvfile:
    reader = csv.DictReader(csvfile)
    to_db = [(i['event_time'], i['event_type'], i['product_id'], i['category_id'], i['category_code'], i['brand'], i['price'], i['user_id'], i['user_session']) for i in reader]

cur.executemany("INSERT INTO ecommerce ecommerce (event_time, event_type, product_id, category_id, category_code, brand, price, user_id, user_session) VALUES (?,?,?,?,?,?,?,?,?);", to_db)
con.commit()

res = cur.execute("SELECT * FROM ecommerce")
res.fetchall()
