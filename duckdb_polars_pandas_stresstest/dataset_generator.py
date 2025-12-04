import os
import shutil
from pathlib import Path
import polars as pl
import duckdb

def generate_dataset(scale_factor: int) -> tuple[float, int]:
    tpc_dir = "tpc"
    parquet_path = f"{tpc_dir}/lineitem_{scale_factor}.parquet"

    print(f"\nGenerating dataset with Scale-Factor {scale_factor} ...\n")
    if os.path.exists(tpc_dir):
        # Remove all files in the directory
        for f in Path(tpc_dir).glob("*.dbb"):
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                shutil.rmtree(f)
    else:
        os.makedirs(tpc_dir)

    # Generate data with DuckDB
    con = duckdb.connect(f"{tpc_dir}/tpc.dbb")
    con.sql("SET temp_directory = '/tmp/duckdb_swap';")
    con.sql("SET max_temp_directory_size = '500GB';")
    con.sql(f"CALL dbgen(sf={scale_factor})")
    con.sql(f"COPY lineitem TO '{parquet_path}' (FORMAT 'parquet');")
    con.close()

    total_bytes = Path(parquet_path).stat().st_size
    total_mb = total_bytes / (1024 ** 2)
    total_gb = total_bytes / (1024 ** 3)
    row_count = pl.scan_parquet(parquet_path).collect().height

    print(f"\nDataset with {row_count:,} rows and {total_gb:.2f} GB (Scale Factor: {scale_factor}) generated!\n")
    return total_mb, row_count

def main():
    for scale_factor in [1] + list(range(10, 100, 10)):
        generate_dataset(scale_factor)

main()