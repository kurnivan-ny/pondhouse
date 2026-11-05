"""Bronze layer: ingestion -> Delta bronze with metadata.

source_format:
  "parquet" — Sling landing (s3://lake/ingestion/{table}.parquet)
  "delta"   — custom ingestion (s3://lake/ingestion/{table})

Adds `_ingested_at` timestamp and writes Delta to s3://lake/bronze/{table}.
"""
import argparse
import os

import ibis

from config import S3, TABLES
from conn import get_conn, read_delta, read_parquet, write_delta


# Types from sql/00_init.sql. Sling and the custom path land the same data with
# different types (e.g. decimal(24,6) vs decimal(10,2)), and ClickHouse refuses a
# table whose files disagree, so bronze fixes one schema whichever path ran.
SOURCE_SCHEMAS = {
    "stores": {"store_id": "int32", "store_name": "string", "city": "string",
               "country_code": "string"},
    "products": {"product_id": "int32", "product_name": "string",
                 "category_code": "string", "unit_price": "decimal(10, 2)"},
    "customers": {"customer_id": "int32", "full_name": "string", "email": "string",
                  "country_code": "string", "signup_date": "date"},
    "transactions": {"transaction_id": "int32", "store_id": "int32", "customer_id": "int32",
                     "transaction_date": "date", "status_code": "string",
                     "payment_method": "string"},
    "transaction_items": {"item_id": "int32", "transaction_id": "int32", "product_id": "int32",
                          "quantity": "int32", "unit_price": "decimal(10, 2)"},
    "category_ref": {"category_code": "string", "category_name": "string"},
    "status_ref": {"status_code": "string", "status_name": "string"},
    "country_ref": {"country_code": "string", "country_name": "string"},
}


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
            src = _read_ingestion(con, table, source_format).cast(SOURCE_SCHEMAS[table])
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
