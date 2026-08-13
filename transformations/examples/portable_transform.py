"""Ibis demo: ONE expression, TWO engines (DuckDB lake + ClickHouse serving).

This is the core pondhouse pattern: write transformation logic once in Ibis,
execute it wherever the data lives.

Run after:  pip install -r ../requirements.txt  &&  docker compose up -d
"""
import ibis


def transform(t: ibis.Table) -> ibis.Table:
    """Portable business logic — engine-agnostic."""
    return (
        t.group_by("id")
        .agg(total=t["value"].sum())
        .order_by(ibis.desc("total"))
    )


# --- Lake engine (DuckDB) ---
duck = ibis.duckdb.connect()
# duck.raw_sql("CREATE SECRET rustfs (TYPE s3, KEY_ID 'pondhouse', ...)")
# t = duck.read_parquet("s3://cdc-landing/**/*.parquet")
# print(duck.execute(transform(t)))

# --- Serving engine (ClickHouse) — SAME expression ---
ch = ibis.clickhouse.connect(
    host="localhost", port=8123,
    user="default", password="pondhouse", database="marts",
)
# print(ch.execute(transform(ch.table("my_mart"))))
