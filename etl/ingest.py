"""Ingestion layer: oltp (postgres) -> s3://lake/ingestion/...

Primary: Sling batch replication to parquet.
Fallback: custom Ibis/DuckDB ingestion directly to Delta, supporting
overwrite / append modes.

Usage:
  python ingest.py              # Sling (default)
  python ingest.py --method custom --mode append
"""
import argparse
import os
import shutil
import subprocess

from config import S3, s3_storage_options
from conn import get_conn, read_pg, vacuum_delta, write_delta

PONDHOUSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
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

# Primary key mapping for each table (for future merge implementation)
PRIMARY_KEYS = {
    "stores": "store_id",
    "products": "product_id",
    "customers": "customer_id",
    "transactions": "transaction_id",
    "transaction_items": "item_id",
    "category_ref": "category_code",
    "status_ref": "status_code",
    "country_ref": "country_code",
}


def sling_available():
    """True when this process can drive the Sling container via the docker CLI."""
    return shutil.which("docker") is not None


def run_sling():
    """Run the Sling replication: oltp.* -> s3://lake/ingestion/{table}.parquet"""
    if not sling_available():
        raise RuntimeError(
            "the docker CLI is not available, so the Sling container cannot be "
            "driven from here (this is the case inside the etl image). Use "
            "--ingest custom, which performs the equivalent ingestion with Ibis."
        )
    subprocess.run(
        ["docker", "compose", "--profile", "tools", "up", "-d", "sling"],
        check=True, cwd=PONDHOUSE_DIR,
    )
    subprocess.run(
        ["docker", "compose", "--profile", "tools", "exec", "-T", "sling",
         "sling", "run", "--home-dir", "/sling", "-r", "/sling/replications/demo-to-lake.yaml"],
        check=True, cwd=PONDHOUSE_DIR,
    )
    print("ingested oltp.* -> s3://lake/ingestion/*.parquet (Sling)")


def _merge_table(con, src, path, table):
    """Upsert `src` into the Delta table at `path` on its primary key.

    Falls back to an overwrite when the target does not exist yet, since there
    is nothing to merge against on the first run.
    """
    import deltalake

    key = PRIMARY_KEYS[table]
    arrow = con.to_pyarrow(src)
    storage_options = s3_storage_options()

    if not deltalake.DeltaTable.is_deltatable(path, storage_options=storage_options):
        deltalake.write_deltalake(path, arrow, mode="overwrite",
                                  schema_mode="overwrite",
                                  storage_options=storage_options)
        vacuum_delta(path, storage_options)
        print(f"ingestion.{table} created ({arrow.num_rows} rows)")
        return

    dt = deltalake.DeltaTable(path, storage_options=storage_options)
    (
        dt.merge(
            source=arrow,
            predicate=f"target.{key} = source.{key}",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )
    vacuum_delta(path, storage_options)
    print(f"ingestion.{table} merged on {key} ({arrow.num_rows} rows)")


def run_custom(mode="overwrite"):
    """Custom ingestion with Ibis/DuckDB: oltp -> Delta ingestion layer.

    Modes:
      overwrite  — replace the ingestion Delta table
      append     — append new rows (no dedup; re-runs duplicate rows)
      merge      — upsert on the table's primary key (idempotent re-runs)
    """
    if mode not in ("overwrite", "append", "merge"):
        raise ValueError(f"mode must be overwrite|append|merge, got {mode}")

    # This is the only step that reads the OLTP source.
    con = get_conn(attach_pg=True)
    try:
        for table in TABLES:
            src = read_pg(con, table)
            path = f"s3://{S3['lake_bucket']}/ingestion/{table}"

            if mode == "overwrite":
                write_delta(con, src, "ingestion", table)
            elif mode == "append":
                import deltalake
                arrow = con.to_pyarrow(src)
                # schema_mode="merge" lets an added source column land without
                # failing the run with a SchemaMismatchError.
                deltalake.write_deltalake(path, arrow, mode="append",
                                          schema_mode="merge",
                                          storage_options=s3_storage_options())
                print(f"ingestion.{table} appended ({arrow.num_rows} rows)")
            elif mode == "merge":
                _merge_table(con, src, path, table)
    finally:
        try:
            con.disconnect()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--method", choices=["sling", "custom"], default="sling")
    parser.add_argument("--mode", choices=["overwrite", "append", "merge"], default="overwrite")
    args = parser.parse_args()

    if args.method == "sling":
        run_sling()
    else:
        run_custom(args.mode)


if __name__ == "__main__":
    main()
