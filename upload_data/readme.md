# Efficiently Uploading Large CSV Files to MotherDuck with Python

This project demonstrates how to efficiently upload a large CSV file to [MotherDuck](https://motherduck.com/) using a Python script. It covers best practices for file formats, performance optimization, and chunked data loading using PyArrow and DuckDB.

---

## File Format Considerations

Choosing the right file format is crucial for performance and scalability when loading data into MotherDuck. Here’s a quick comparison:

### **Parquet (Recommended)**
- **Compression:** 5–10× better than CSV
- **Performance:** 5–10× higher throughput due to compression
- **Schema:** Self-describing with embedded metadata
- **Best for:** Production data loading, large datasets

### **CSV**
- **Compression:** Minimal
- **Performance:** Slower, especially for large files
- **Schema:** Requires manual type inference or specification
- **Best for:** Simple data exploration, small datasets

### **JSON**
- **Compression:** Moderate
- **Performance:** Slower than Parquet due to parsing overhead
- **Schema:** Flexible, but requires careful type handling
- **Best for:** Semi-structured data, API responses

---

## Performance Optimization Strategies

- **Batch Size:** The optimal batch size is around 1,000,000 rows. DuckDB processes data in groups of 122,800 rows and will parallelize across 10 threads at this scale. Performance is similar for batch sizes between 100,000 and 1,000,000 rows.
- **DataFrames:** Use Pandas, Polars, or PyArrow DataFrames as in-memory buffers for bulk loading. These libraries are optimized for high-throughput inserts.

---

## Python Implementation

The following code demonstrates:
- Automatic schema inference from the CSV file
- Dynamic PyArrow and DuckDB schema creation
- Chunked loading of data into MotherDuck

```python
import os
import math
import logging
import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
from ArrowTableLoadingBuffer import ArrowTableLoadingBuffer
import utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Step 1: Infer schema from CSV using DuckDB
con = duckdb.connect('')
csv_path = f"{utils.get_dataset_dir()}/ecommerce.csv"
con.sql(f"CREATE TABLE tmp AS SELECT * FROM read_csv('{csv_path}')")
tmp_description = con.sql("DESCRIBE tmp").fetchall()

# Step 2: Map SQL types to PyArrow types
SQL_TO_PA = {
    'INTEGER': pa.int32(),
    'BIGINT': pa.int64(),
    'VARCHAR': pa.string(),
    'TEXT': pa.string(),
    'FLOAT': pa.float32(),
    'DOUBLE': pa.float64(),
    'BOOLEAN': pa.bool_(),
    # Extend as needed
}

pa_fields = []
duck_fields = []
for col_name, sql_type, *_ in tmp_description:
    base_type = sql_type.upper().split('(')[0]
    pa_type = SQL_TO_PA.get(base_type, pa.string())
    pa_fields.append((col_name, pa_type))
    duck_fields.append((col_name, base_type))

pyarrow_schema = pa.schema(pa_fields)
duckdb_schema = "CREATE TABLE IF NOT EXISTS my_table (" + ", ".join(f"{n} {t}" for n, t in duck_fields) + ")"

# Step 3: Read CSV as PyArrow Table
arrow_table = pacsv.read_csv(csv_path, read_options=pacsv.ReadOptions(), convert_options=pacsv.ConvertOptions(column_types=pyarrow_schema))

# Step 4: Calculate optimal chunk size
total_rows = con.sql(f"SELECT COUNT(*) FROM read_csv('{csv_path}')").fetchall()[0][0]
chunk_size = math.ceil(total_rows / 100_000)  # Adjust denominator as needed

# Step 5: Load data in chunks to MotherDuck
loader = ArrowTableLoadingBuffer(
    duckdb_schema=duckdb_schema,
    pyarrow_schema=pyarrow_schema,
    database_name="my_db",
    table_name="my_table",
    destination="md",
    chunk_size=chunk_size
)
loader.insert(arrow_table)
```

---

## `ArrowTableLoadingBuffer` Class

This helper class manages chunked inserts and connection setup for both local DuckDB and MotherDuck.

```python
class ArrowTableLoadingBuffer:
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pa.Schema,
        database_name: str,
        table_name: str,
        destination="local",
        chunk_size: int = 100_000,
    ):
        self.duckdb_schema = duckdb_schema
        self.pyarrow_schema = pyarrow_schema
        self.database_name = database_name
        self.table_name = table_name
        self.total_inserted = 0
        self.conn = self.initialize_connection(destination, duckdb_schema)
        self.chunk_size = chunk_size

    def initialize_connection(self, destination, sql):
        if destination == "md":
            logging.info("Connecting to MotherDuck...")
            if not os.environ.get("MOTHERDUCK_TOKEN"):
                raise ValueError("MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'.")
            conn = duckdb.connect("md:")
            logging.info(f"Creating database {self.database_name} if it doesn't exist")
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            conn.execute(f"USE {self.database_name}")
        else:
            conn = duckdb.connect(database=f"{self.database_name}.db")
        conn.execute(sql)
        return conn

    def insert(self, table: pa.Table):
        total_rows = table.num_rows
        for batch_start in range(0, total_rows, self.chunk_size):
            batch_end = min(batch_start + self.chunk_size, total_rows)
            chunk = table.slice(batch_start, batch_end - batch_start)
            self.insert_chunk(chunk)
            logging.info(f"Inserted chunk {batch_start} to {batch_end}")
        self.total_inserted += total_rows
        logging.info(f"Total inserted: {self.total_inserted} rows")

    def insert_chunk(self, chunk: pa.Table):
        self.conn.register("buffer_table", chunk)
        insert_query = f"INSERT INTO {self.table_name} SELECT * FROM buffer_table"
        self.conn.execute(insert_query)
        self.conn.unregister("buffer_table")
```

---

## **Summary**

- **Use Parquet for production and large datasets.**
- **Leverage DataFrame libraries for efficient in-memory buffering and bulk inserts.**
- **Chunk data for optimal performance, especially with large files.**
- **Automate schema inference and conversion for robust, reusable pipelines.**
