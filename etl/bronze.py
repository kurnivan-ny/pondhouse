"""Bronze layer: ingestion -> Delta bronze with metadata.

source_format:
  "parquet" — Sling landing (s3://lake/ingestion/{table}.parquet)
  "delta"   — custom ingestion (s3://lake/ingestion/{table})

Adds `_ingested_at` timestamp and writes Delta to s3://lake/bronze/{table}.
"""
import argparse
import os

import ibis

from config import S3
from conn import get_conn, read_delta, read_parquet, write_delta

TABLES = [
    "stores",
    "products",
    "customers",
    "transactions",
    "transaction_items",
    "category_ref",
    "status_ref",
    "country_ref",
]


def _read_ingestion(con, table, source_format):
    path = f"s3://{S3['lake_bucket']}/ingestion/{table}"
    view = f"ingestion_{table}"
    if source_format == "delta":
        return read_delta(con, path, view)
    return read_parquet(con, f"{path}.parquet", view)


def run_bronze(source_format="parquet"):
    con = get_conn()
    try:
        for table in TABLES:
            src = _read_ingestion(con, table, source_format)
            # One batch-level load timestamp: ibis.now() is transaction-constant,
            # so every row of a given run shares the same _ingested_at value.
            bronze = src.mutate(_ingested_at=ibis.now())
            write_delta(con, bronze, "bronze", table)
    finally:
        try:
            con.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-format", choices=["parquet", "delta"],
        default=os.getenv("BRONZE_SOURCE_FORMAT", "parquet"),
        help="layout of the ingestion layer (default: parquet, as written by Sling)",
    )
    args = parser.parse_args()
    run_bronze(source_format=args.source_format)


if __name__ == "__main__":
    main()
