"""Bronze layer: Sling's parquet landing -> Delta bronze with metadata.

Reads s3://lake/ingestion/{table}.parquet, adds an `_ingested_at` timestamp and writes
Delta to s3://lake/bronze/{table}. In INGESTION_MODE=cdc, it merges new Debezium events
instead (see cdc.py).
"""
import ibis

import cdc
from config import INGESTION_MODE, S3, TABLES
from conn import get_conn, read_parquet, write_delta


# Types from sql/00_init.sql. Sling lands wider types than the source (e.g.
# decimal(24,6) for decimal(10,2)), so bronze restores them. That keeps every later
# layer's schema stable.
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
    "refunds": {"refund_id": "int32", "item_id": "int32", "refund_date": "date",
                "quantity": "int32", "amount": "decimal(10, 2)", "reason": "string"},
    "category_ref": {"category_code": "string", "category_name": "string"},
    "status_ref": {"status_code": "string", "status_name": "string"},
    "country_ref": {"country_code": "string", "country_name": "string"},
}


def build_table(con, table):
    if INGESTION_MODE == "cdc":
        cdc.apply_events(con, table, SOURCE_SCHEMAS[table])
        return
    path = f"s3://{S3['lake_bucket']}/ingestion/{table}.parquet"
    src = read_parquet(con, path, f"ingestion_{table}").cast(SOURCE_SCHEMAS[table])
    # One batch-level load timestamp: ibis.now() is transaction-constant,
    # so every row of a given run shares the same _ingested_at value.
    write_delta(con, src.mutate(_ingested_at=ibis.now()), "bronze", table)


def run_bronze():
    con = get_conn()
    try:
        for table in TABLES:
            build_table(con, table)
    finally:
        try:
            con.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    run_bronze()
