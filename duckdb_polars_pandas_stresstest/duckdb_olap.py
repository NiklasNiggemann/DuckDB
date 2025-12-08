from memory_profiler import profile
import duckdb

@profile
def pricing_summary_report(scale_factor: int):
    parquet_path = f'tpc/lineitem_{scale_factor}.parquet'
    con = duckdb.connect()
    query = f"""
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        read_parquet('{parquet_path}')
    where
        l_shipdate <= '1998-09-02'
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus
    """

    pl_df = con.execute(query).pl()
    print(pl_df)
    con.close()