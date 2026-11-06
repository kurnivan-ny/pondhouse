"""Data-quality checks on the lake and the serving layer.

Dagster runs these as asset checks. Each returns {"passed": bool, "metadata": dict}.

Usage:
  python checks.py {ingestion|bronze|gold|marts}   # prints the result as JSON
"""
import argparse
import json

import serve
import transform
from config import INGESTION_MODE, OPTIONAL_TABLES, S3, TABLES
from conn import get_conn, read_parquet

FACT_FOREIGN_KEYS = [
    ("customer_key", "dim_customer"),
    ("product_key", "dim_product"),
    ("store_key", "dim_store"),
    ("date_key", "dim_date"),
]


def _count(table):
    return int(table.count().execute())


def check_table_rows(read, layer, table):
    """One source table must have rows in `layer`."""
    rows = _count(read(layer, table))
    return {"passed": rows > 0, "metadata": {"rows": rows}}


def check_row_counts(read, layer):
    """Every source table must have rows in `layer`."""
    counts = {table: _count(read(layer, table)) for table in TABLES}
    empty = [table for table, n in counts.items() if n == 0 and table not in OPTIONAL_TABLES]
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
    refunds = read("gold", "fact_refunds")
    refund_orphans = {
        key: _count(refunds.anti_join(read("gold", dim), key))
        for key, dim in FACT_FOREIGN_KEYS
    }
    refunds_of_unknown_items = _count(refunds.anti_join(fact, "item_key"))
    current_per_customer = read("gold", "dim_customer")
    current_per_customer = current_per_customer.filter(current_per_customer.is_current)
    customers_without_one_current_version = (
        _count(current_per_customer) - int(current_per_customer.customer_id.nunique().execute())
    )
    fact_revenue = float(fact.line_amount.sum().execute() or 0)
    silver_revenue = float(sales.line_amount.sum().execute() or 0)
    rejected = _count(read("silver", "rejected_sales"))

    return {
        "passed": (
            rows > 0
            and duplicates == 0
            and not any(orphans.values())
            and not any(refund_orphans.values())
            and refunds_of_unknown_items == 0
            and customers_without_one_current_version == 0
            and round(fact_revenue - silver_revenue, 2) == 0
        ),
        "metadata": {
            "rows": rows,
            "duplicate_item_keys": duplicates,
            "orphan_keys": orphans,
            "refund_orphan_keys": refund_orphans,
            "refunds_of_unknown_items": refunds_of_unknown_items,
            "customers_without_one_current_version": customers_without_one_current_version,
            "fact_revenue": round(fact_revenue, 2),
            "silver_revenue": round(silver_revenue, 2),
            # Rejections are for inspection: they do not fail the check.
            "rejected_sales": rejected,
        },
    }


def check_served_table(read, table, clickhouse_count):
    """A ClickHouse table must serve the same row count as its Delta table."""
    layer, _sort_key = serve.MARTS[table]
    delta = _count(read(layer, table))
    served = clickhouse_count(table)
    return {"passed": delta == served, "metadata": {"delta": delta, "clickhouse": served}}


def check_marts(read, clickhouse_count):
    """Every ClickHouse table must serve the same row count as its Delta table."""
    results = {t: check_served_table(read, t, clickhouse_count) for t in serve.MARTS}
    mismatched = [t for t, result in results.items() if not result["passed"]]
    return {
        "passed": not mismatched,
        "metadata": {
            "delta": {t: result["metadata"]["delta"] for t, result in results.items()},
            "clickhouse": {t: result["metadata"]["clickhouse"] for t, result in results.items()},
            "mismatched": mismatched,
        },
    }


def _clickhouse_count(table):
    return int(serve.execute(f"SELECT count() FROM marts.{table}").strip())


def lake_reader(con):
    """read(layer, table) over the lake: Sling's parquet landing, Delta after that."""
    def read(layer, table):
        if layer == "ingestion":
            path = f"s3://{S3['lake_bucket']}/ingestion/{table}.parquet"
            return read_parquet(con, path, f"ingestion_{table}")
        return transform._read(con, layer, table)
    return read


def run_table_rows(layer, table):
    if layer == "ingestion" and INGESTION_MODE == "cdc":
        return {"passed": True, "metadata": {"skipped": "INGESTION_MODE=cdc lands no parquet"}}
    con = get_conn()
    try:
        return check_table_rows(lake_reader(con), layer, table)
    finally:
        con.disconnect()


def run_served_table(table):
    con = get_conn()
    try:
        return check_served_table(lake_reader(con), table, _clickhouse_count)
    finally:
        con.disconnect()


def run(layer):
    if layer == "ingestion" and INGESTION_MODE == "cdc":
        return {"passed": True, "metadata": {"skipped": "INGESTION_MODE=cdc lands no parquet"}}
    con = get_conn()
    try:
        read = lake_reader(con)
        if layer in ("ingestion", "bronze"):
            return check_row_counts(read, layer)
        if layer == "gold":
            return check_gold(read)
        return check_marts(read, _clickhouse_count)
    finally:
        con.disconnect()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("layer", choices=["ingestion", "bronze", "gold", "marts"])
    args = parser.parse_args()
    print(json.dumps(run(args.layer)))


if __name__ == "__main__":
    main()
