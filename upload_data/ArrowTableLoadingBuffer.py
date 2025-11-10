import duckdb
import os
import pyarrow as pa
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ArrowTableLoadingBuffer:
    def __init__(
        self,
        duckdb_schema: str,
        pyarrow_schema: pa.Schema,
        database_name: str,
        table_name: str,
        destination="local",
        chunk_size: int = 100_000, # Default chunk size
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
                raise ValueError(
                    "MotherDuck token is required. Set the environment variable 'MOTHERDUCK_TOKEN'."
                )
            conn = duckdb.connect("md:")
            logging.info(
                f"Creating database {self.database_name} if it doesn't exist"
            )
            conn.execute(f"CREATE DATABASE IF NOT EXISTS {self.database_name}")
            conn.execute(f"USE {self.database_name}")
        else:
            conn = duckdb.connect(database=f"{self.database_name}.db")
        conn.execute(sql)  # Execute schema setup on initialization
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