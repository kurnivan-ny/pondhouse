"""Backend compatibility: the same Ibis expressions on DuckDB and ClickHouse.

The pondhouse premise is "write the logic once, run it where the data lives".
These tests compile the real transform expressions against both backends and,
when a ClickHouse server is reachable, execute the full medallion build on it
and compare the results row-for-row with DuckDB.

ClickHouse tests skip automatically when no server is available.
"""
import pandas as pd
import pytest

import transform


LAYERS = [
    ("silver", "customers"), ("silver", "products"), ("silver", "stores"),
    ("silver", "transactions"), ("silver", "sales"),
    ("gold", "dim_customer"), ("gold", "dim_product"), ("gold", "dim_store"),
    ("gold", "dim_date"), ("gold", "fact_sales"),
    ("mart", "daily_sales_mart"), ("mart", "product_sales_mart"),
    ("mart", "customer_sales_mart"),
]

SORT_KEYS = {
    ("silver", "customers"): ["customer_id"],
    ("silver", "products"): ["product_id"],
    ("silver", "stores"): ["store_id"],
    ("silver", "transactions"): ["transaction_id"],
    ("silver", "sales"): ["item_id"],
    ("gold", "dim_customer"): ["customer_key"],
    ("gold", "dim_product"): ["product_key"],
    ("gold", "dim_store"): ["store_key"],
    ("gold", "dim_date"): ["date_key"],
    ("gold", "fact_sales"): ["item_key"],
    ("mart", "daily_sales_mart"): ["date_key", "store_name"],
    ("mart", "product_sales_mart"): ["category", "product_name"],
    ("mart", "customer_sales_mart"): ["customer_key"],
}


def _normalize(df, keys):
    df = df.sort_values(keys).reset_index(drop=True)
    df = df[sorted(df.columns)]
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].astype("float64").round(2)
        else:
            df[col] = df[col].astype(str)
    return df


def _build(lake):
    transform.build_silver(lake.con)
    transform.build_gold(lake.con)
    transform.build_mart(lake.con)
    return lake


def test_expressions_compile_on_duckdb(lake):
    """Every layer must compile to SQL on DuckDB without unbound references."""
    _build(lake)
    for layer, table in LAYERS:
        sql = lake.con.compile(lake.tables[(layer, table)])
        assert sql


def test_expressions_compile_on_clickhouse(ch_lake):
    """Same Ibis expressions must also compile on the ClickHouse backend."""
    _build(ch_lake)
    for layer, table in LAYERS:
        sql = ch_lake.con.compile(ch_lake.tables[(layer, table)])
        assert sql


@pytest.mark.parametrize("layer,table", LAYERS)
def test_duckdb_and_clickhouse_agree(built, ch_lake, layer, table):
    """Identical logic must produce identical data on both engines."""
    _build(ch_lake)
    keys = SORT_KEYS[(layer, table)]
    duck_df = _normalize(built.df(layer, table), keys)
    ch_df = _normalize(ch_lake.df(layer, table), keys)
    pd.testing.assert_frame_equal(duck_df, ch_df, check_dtype=False)
