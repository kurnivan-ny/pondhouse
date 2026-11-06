"""Ibis + DuckDB lake connection helper.

Creates one DuckDB connection that can read Postgres (source), read/write Delta
on RustFS (lake), and run Ibis expressions. The connection is in-memory: all state
lives in the lake, so concurrent processes cannot conflict over a DuckDB file.
"""
import os

import ibis

from config import DB, S3


def _s3_secret_sql():
    # DuckDB S3 secret: USE_SSL should match the scheme
    use_ssl = S3.get("scheme", "http") == "https"
    return (
        "CREATE OR REPLACE SECRET rustfs (TYPE s3,"
        f" KEY_ID '{S3['access_key']}', SECRET '{S3['secret_key']}',"
        f" ENDPOINT '{S3['endpoint']}', URL_STYLE 'path', USE_SSL {str(use_ssl).lower()});"
    )


def _pg_attach_sql():
    return (
        "ATTACH"
        f" 'host={DB['host']} port={DB['port']} dbname={DB['dbname']}"
        f" user={DB['user']} password={DB['password']}'"
        " AS pg (TYPE postgres, READ_ONLY);"
    )


def _install_extension(con, name):
    """Install and load a DuckDB extension with error handling."""
    try:
        con.raw_sql(f"INSTALL {name}; LOAD {name};")
    except Exception as e:
        raise RuntimeError(f"Failed to install/load DuckDB extension '{name}': {e}") from e


def get_conn(attach_pg=None):
    """Return an Ibis DuckDB connection wired for S3 Delta (and optionally Postgres).

    Only the ingestion step reads Postgres; bronze/silver/gold/mart work purely
    on the lake. Attaching unconditionally makes those steps fail whenever
    Postgres is down, so the attach is opt-in. Default: attach only when
    ETL_ATTACH_PG is truthy, and never fail the connection if it does not work.
    """
    if attach_pg is None:
        attach_pg = os.getenv("ETL_ATTACH_PG", "0").lower() in ("1", "true", "yes")

    con = ibis.duckdb.connect()
    _install_extension(con, "delta")
    con.raw_sql(_s3_secret_sql())

    if attach_pg:
        _install_extension(con, "postgres")
        con.raw_sql(_pg_attach_sql())
    return con


def read_pg(con, table, schema="pos"):
    """Read a Postgres table through the attached connection as an Ibis table."""
    view = f"src_{table}"
    con.raw_sql(f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM pg.{schema}.{table};")
    return con.table(view)


def read_parquet(con, path, table_name):
    """Read a parquet file (local or s3://) as an Ibis table."""
    con.raw_sql(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{path}');")
    return con.table(table_name)


def read_delta(con, path, table_name):
    """Read a Delta table (local or s3://) as an Ibis table."""
    con.raw_sql(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM delta_scan('{path}');")
    return con.table(table_name)


def vacuum_delta(path, storage_options):
    """Drop data files that the current Delta version no longer references.

    An overwrite/merge only tombstones the previous files in the transaction
    log; they stay in the bucket. On a frequent schedule they would pile up, so
    this removes them. ClickHouse 26.3 follows the log and reads correctly without
    this, unlike older versions, which failed on leftover files.

    retention_hours=0 needs the safety check disabled; this is a rebuild-in-place
    pipeline with a single writer and no readers of older versions.
    """
    import deltalake

    deltalake.DeltaTable(path, storage_options=storage_options).vacuum(
        retention_hours=0,
        enforce_retention_duration=False,
        dry_run=False,
    )


def write_delta(con, expr, layer, table, vacuum=True):
    """Execute an Ibis expression and write the result as a Delta table.

    Vacuums afterwards so the directory matches the transaction log (see
    vacuum_delta).
    """
    import deltalake
    from config import s3_storage_options

    arrow = con.to_pyarrow(expr)
    path = f"s3://{S3['lake_bucket']}/{layer}/{table}"
    storage_options = s3_storage_options()
    deltalake.write_deltalake(path, arrow, mode="overwrite", schema_mode="overwrite",
                              storage_options=storage_options)

    if vacuum:
        vacuum_delta(path, storage_options)
    print(f"{layer}.{table} written")
