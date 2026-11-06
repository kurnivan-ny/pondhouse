"""Serving step: load the gold + mart Delta tables into native ClickHouse tables.

Each table is built under a temporary name, then swapped in with EXCHANGE TABLES. The
swap is atomic, so queries never see a half-loaded table, and a failed build leaves the
previous data serving. Once loaded, the marts do not need RustFS to answer queries.

Statements are sent over the ClickHouse HTTP interface, so this works the same
on the host and inside a container (no docker socket / docker CLI required).

Usage:
  python serve.py
"""
import os
import urllib.error
import urllib.request

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT", "8123")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "pondhouse")

# As ClickHouse reaches the lake, inside the compose network. Credentials come from
# the `rustfs` named collection (serving/config.d/rustfs_s3.xml).
LAKE_URL = "http://rustfs:9000/lake"

# Served table -> (lake layer, ClickHouse sort key)
MARTS = {
    "dim_customer": ("gold", "customer_key"),
    "dim_product": ("gold", "product_key"),
    "dim_store": ("gold", "store_key"),
    "dim_date": ("gold", "date_key"),
    "fact_sales": ("gold", "(date_key, item_key)"),
    "daily_sales_mart": ("mart", "(date_key, store_name)"),
    "product_sales_mart": ("mart", "(category, product_name)"),
    "customer_sales_mart": ("mart", "customer_key"),
}


def execute(statement, timeout=120):
    """Run one statement against ClickHouse over HTTP, raising on error."""
    # Credentials go in headers: a query string can end up in logs.
    request = urllib.request.Request(
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/",
        data=statement.encode("utf-8"),
        headers={
            "X-ClickHouse-User": CLICKHOUSE_USER,
            "X-ClickHouse-Key": CLICKHOUSE_PASSWORD,
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace").strip()
        raise RuntimeError(f"ClickHouse rejected statement: {detail}") from e
    except urllib.error.URLError as e:
        raise RuntimeError(
            f"cannot reach ClickHouse at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}: {e.reason}"
        ) from e


def serve_table(table):
    """Rebuild one served table from the lake and swap it in atomically."""
    layer, sort_key = MARTS[table]
    target = f"marts.{table}"
    new = f"marts.{table}__new"

    execute("CREATE DATABASE IF NOT EXISTS marts")
    # A build that died earlier may have left this behind.
    execute(f"DROP TABLE IF EXISTS {new}")
    # Delta columns arrive as Nullable, hence allow_nullable_key.
    execute(
        f"CREATE TABLE {new} ENGINE = MergeTree ORDER BY {sort_key} "
        "SETTINGS allow_nullable_key = 1 AS "
        f"SELECT * FROM deltaLake(rustfs, url = '{LAKE_URL}/{layer}/{table}/')"
    )
    # EXCHANGE needs both tables to exist; on the first run the target does not.
    execute(f"CREATE TABLE IF NOT EXISTS {target} AS {new}")
    execute(f"EXCHANGE TABLES {new} AND {target}")
    execute(f"DROP TABLE {new}")
    print(f"marts.{table} swapped in")


def run_serve():
    for table in MARTS:
        serve_table(table)


if __name__ == "__main__":
    run_serve()
