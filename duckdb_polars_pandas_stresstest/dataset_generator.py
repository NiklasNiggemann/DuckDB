import os
import shutil
from pathlib import Path
import polars as pl
import duckdb

def generate_dataset(scale_factor: int):
    tpc_dir = "tpc"
    parquet_path = f"{tpc_dir}/lineitem_{scale_factor}.parquet"

    print(f"\nGenerating dataset with Scale-Factor {scale_factor} ...\n")
    if os.path.exists(tpc_dir):
        for f in Path(tpc_dir).glob("*.dbb"):
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                shutil.rmtree(f)
    else:
        os.makedirs(tpc_dir)

    # Generate data with DuckDB
    con = duckdb.connect(f"{tpc_dir}/tpc.dbb")
    con.sql(f"CALL dbgen(sf={scale_factor})")
    con.sql(f"COPY lineitem TO '{parquet_path}' (FORMAT 'parquet');")
    con.close()

    if os.path.exists(tpc_dir):
        for f in Path(tpc_dir).glob("*.dbb"):
            if f.is_file():
                f.unlink()
            elif f.is_dir():
                shutil.rmtree(f)
    else:
        os.makedirs(tpc_dir)

    print(f"\nDataset with Scale Factor: {scale_factor} generated!\n")

def main():
    for scale_factor in (10, 20, 40, 80, 160, 320, 640):
        generate_dataset(scale_factor)
    # generate_dataset(1280)

main()