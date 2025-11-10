import pyarrow
import duckdb
from pyarrow import csv
import utils
from ArrowTableLoadingBuffer import ArrowTableLoadingBuffer
import pyarrow as pa
import math

# For bulk inserts, using dataframe libraries is the easiest way, as they are optimized.
# A Pandas/Polars/PyArrow DataFrame can be used as an in memory buffer before bulk loading

con = duckdb.connect('')
con.sql(f"CREATE TABLE tmp AS SELECT * FROM read_csv('{utils.get_dataset_dir()}/ecommerce.csv')")
tmp_description = con.sql("DESCRIBE tmp").fetchall()

SQL_TO_PA = {
    'INTEGER': pa.int32(),
    'BIGINT': pa.int64(),
    'VARCHAR': pa.string(),
    'TEXT': pa.string(),
    'FLOAT': pa.float32(),
    'DOUBLE': pa.float64(),
    'BOOLEAN': pa.bool_(),
    # Add more as needed
}

duck_fields = []
pa_fields = []
for row in tmp_description:
    col_name = row[0]
    sql_type = row[1].upper()
    base_type = sql_type.split('(')[0]  # This is a string!
    pa_type = SQL_TO_PA.get(base_type, pa.string())  # Default to string
    pa_fields.append((col_name, pa_type))

# create a pyarrow table from the csv file
arrow_table = pyarrow.csv.read_csv(f"{utils.get_dataset_dir()}/ecommerce.csv")

# Define the DuckDB schema as a DDL statement
ddl = ', '.join([f"{col_name} {sql_type}" for col_name, sql_type in pa_fields])
duckdb_schema = f"CREATE TABLE IF NOT EXISTS my_table ({ddl})"
pyarrow_schema = pa.schema(pa_fields)

loader = ArrowTableLoadingBuffer(
    duckdb_schema=duckdb_schema,
    pyarrow_schema=pyarrow_schema,
    database_name="my_db",
    table_name="my_table",
    destination="md",
    # recommended chunk size is typically 100k rows; so we calculate the individual chunk size accordingly to the dataset
    chunk_size= math.ceil(con.sql(f"SELECT COUNT(*) FROM read_csv('{utils.get_dataset_dir()}/ecommerce.csv')").fetchall()[0][0] / 100000)
)

# Load the data
loader.insert(arrow_table)

