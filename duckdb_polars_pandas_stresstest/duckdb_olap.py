import duckdb
import utils
from memory_profiler import profile

parquet_path = 'tpc/lineitem.parquet'

@profile
def total_sales_per_region():
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) AS sum_qty,
            SUM(l_extendedprice) AS sum_base_price,
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
            SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
            AVG(l_quantity) AS avg_qty,
            AVG(l_extendedprice) AS avg_price,
            AVG(l_discount) AS avg_disc,
            COUNT(*) AS count_order
        FROM
            read_parquet('{parquet_path}')
        WHERE
            l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
        GROUP BY
            l_returnflag,
            l_linestatus
        ORDER BY
            l_returnflag,
            l_linestatus;
    """).fetchall()

    for row in result:
        print(row)
    con.close()


