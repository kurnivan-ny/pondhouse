"""Data-quality checks on the lake and the serving layer.

Dagster runs these as asset checks. Each returns {"passed": bool, "metadata": dict}.

Usage:
  python checks.py {ingestion|bronze|gold|marts}   # prints the result as JSON
"""
import argparse
import json
import sys

import transform
from config import S3, TABLES
from conn import get_conn, read_delta, read_parquet

GOLD_TABLES = ["dim_customer", "dim_product", "dim_store", "dim_date", "fact_sales"]
MART_TABLES = ["daily_sales_mart", "product_sales_mart", "customer_sales_mart"]
FACT_FOREIGN_KEYS = [
    ("customer_key", "dim_customer"),
    ("product_key", "dim_product"),
    ("store_key", "dim_store"),
    ("date_key", "dim_date"),
]


def _count(table):
    return int(table.count().execute())


def check_row_counts(read, layer):
    """Every source table must have rows in `layer`."""
    counts = {table: _count(read(layer, table)) for table in TABLES}
    empty = [table for table, n in counts.items() if n == 0]
    return {"passed": not empty, "metadata": {"row_counts": counts, "empty_tables": empty}}


def check_gold(read):
    """fact_sales is non-empty, unique on its grain, resolves against every
    dimension, and carries exactly the revenue silver computed."""
    fact = read("gold", "fact_sales")
    sales = read("silver", "sales")

    rows = _count(fact)
    duplicates = rows - int(fact.item_key.nunique().execute())
    orphans = {
        key: _count(fact.anti_join(read("gold", dim), key))
        for key, dim in FACT_FOREIGN_KEYS
    }
    fact_revenue = float(fact.line_amount.sum().execute() or 0)
    silver_revenue = float(sales.line_amount.sum().execute() or 0)
    rejected = _count(read("silver", "rejected_sales"))

    return {
        "passed": (
            rows > 0
            and duplicates == 0
            and not any(orphans.values())
            and round(fact_revenue - silver_revenue, 2) == 0
        ),
        "metadata": {
            "rows": rows,
            "duplicate_item_keys": duplicates,
            "orphan_keys": orphans,
            "fact_revenue": round(fact_revenue, 2),
            "silver_revenue": round(silver_revenue, 2),
            # Rejections are for inspection: they do not fail the check.
            "rejected_sales": rejected,
        },
    }


def check_marts(read, clickhouse_count):
    """Each ClickHouse table must serve the same row count as its Delta table."""
    expected = {t: _count(read("gold", t)) for t in GOLD_TABLES}
    expected.update({t: _count(read("mart", t)) for t in MART_TABLES})
    served = {t: clickhouse_count(t) for t in expected}
    mismatched = [t for t in expected if expected[t] != served[t]]
    return {
        "passed": not mismatched,
        "metadata": {"delta": expected, "clickhouse": served, "mismatched": mismatched},
    }


def _read_ingestion(con):
    """The ingestion layer is Delta (custom path) or parquet (Sling path)."""
    def read(_layer, table):
        path = f"s3://{S3['lake_bucket']}/ingestion/{table}"
        try:
            return read_delta(con, path, f"ingestion_{table}")
        except Exception:
            return read_parquet(con, f"{path}.parquet", f"ingestion_{table}")
    return read


def _clickhouse_count(table):
    import serve

    return int(serve.execute(f"SELECT count() FROM marts.{table}").strip())


def run(layer):
    con = get_conn()
    try:
        def read(lake_layer, table):
            return transform._read(con, lake_layer, table)

        if layer == "ingestion":
            return check_row_counts(_read_ingestion(con), "ingestion")
        if layer == "bronze":
            return check_row_counts(read, "bronze")
        if layer == "gold":
            return check_gold(read)
        return check_marts(read, _clickhouse_count)
    finally:
        con.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("layer", choices=["ingestion", "bronze", "gold", "marts"])
    args = parser.parse_args()
    result = run(args.layer)
    print(json.dumps(result))
    sys.exit(0)


if __name__ == "__main__":
    main()
