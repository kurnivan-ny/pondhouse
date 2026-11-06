"""Incremental bronze: merge new Debezium events from s3://cdc-landing into bronze Delta.

Used when INGESTION_MODE=cdc. Each bronze row keeps the time of the event behind it in
`_cdc_ts`, and only newer events are read. No other state is needed, because applying an
event twice changes nothing: a delete of a row already gone is simply a no-op.

Switching over from snapshot mode replays the whole topic. Debezium's initial snapshot is
in it, so that rebuilds the table.
"""
import deltalake
import pyarrow.compute as pc

from config import s3_storage_options

LANDING = "s3://cdc-landing/cdc/demo.pos.{table}/*/*.parquet"

# Primary key of each source table: an event's identity, and what merges match on.
PRIMARY_KEYS = {
    "stores": "store_id",
    "products": "product_id",
    "customers": "customer_id",
    "transactions": "transaction_id",
    "transaction_items": "item_id",
    "category_ref": "category_code",
    "status_ref": "status_code",
    "country_ref": "country_code",
    "refunds": "refund_id",
}

_SQL_TYPES = {"int32": "INTEGER", "string": "VARCHAR", "date": "DATE"}


def _sql_type(ibis_type):
    return _SQL_TYPES.get(ibis_type, ibis_type.upper())


def _select_columns(schema, source):
    """The source's columns, typed as bronze keeps them, from an event's `after` struct."""
    columns = []
    for column, column_type in schema.items():
        value = f'{source}."{column}"'
        if column_type == "date":
            # Debezium sends a date as a count of days since 1970.
            columns.append(f"""DATE '1970-01-01' + CAST({value} AS INTEGER) AS "{column}" """)
        else:
            columns.append(f'CAST({value} AS {_sql_type(column_type)}) AS "{column}"')
    return ", ".join(columns)


def latest_events_sql(table, schema, path, since):
    """One row per primary key: its newest event after `since`, with what to do with it."""
    key = PRIMARY_KEYS[table]
    return f"""
        SELECT * EXCLUDE (_rn) FROM (
            SELECT {_select_columns(schema, "value.after")},
                   value.op = 'd' AS _is_delete,
                   COALESCE(value.after."{key}", value.before."{key}") AS _key,
                   now() AS _ingested_at,
                   value.ts_ns AS _cdc_ts,
                   row_number() OVER (
                       PARTITION BY COALESCE(value.after."{key}", value.before."{key}")
                       ORDER BY value.ts_ns DESC
                   ) AS _rn
            FROM read_parquet('{path}')
            WHERE value.ts_ns > {int(since)}
        ) WHERE _rn = 1
    """


def _empty_sql(schema):
    columns = ", ".join(
        f'CAST(NULL AS {_sql_type(column_type)}) AS "{column}"'
        for column, column_type in schema.items()
    )
    return f"""
        SELECT {columns}, now() AS _ingested_at, CAST(NULL AS BIGINT) AS _cdc_ts WHERE false
    """


def _to_arrow(con, sql):
    # Its own cursor: DuckDB allows one open result per connection.
    return con.con.cursor().sql(sql).to_arrow_table()


def _has_events(con, path):
    return con.raw_sql(f"SELECT count(*) FROM glob('{path}')").fetchone()[0] > 0


def apply_events(con, table, schema, target=None, landing=None, storage_options=None):
    """Bring one bronze table up to date. Paths are parameters so tests can use local files."""
    target = target or f"s3://lake/bronze/{table}"
    path = landing or LANDING.format(table=table)
    options = s3_storage_options() if storage_options is None else storage_options

    existing = None
    if deltalake.DeltaTable.is_deltatable(target, storage_options=options):
        existing = deltalake.DeltaTable(target, storage_options=options)
    columns = [field.name for field in existing.schema().fields] if existing else []
    # Without _cdc_ts this is a table from snapshot mode, so replace it from the full topic.
    resuming = "_cdc_ts" in columns

    if not _has_events(con, path):
        # A table with no rows has no topic yet, but later layers still read it.
        if existing is None:
            arrow = _to_arrow(con, _empty_sql(schema))
            deltalake.write_deltalake(target, arrow, storage_options=options)
        print(f"bronze.{table}: no events")
        return

    since = 0
    if resuming:
        arrow_ts = existing.to_pyarrow_table(columns=["_cdc_ts"]).column("_cdc_ts")
        since = max((value for value in arrow_ts.to_pylist() if value is not None), default=0)

    events = _to_arrow(con, latest_events_sql(table, schema, path, since))
    if not resuming:
        rows = events.filter(pc.invert(events["_is_delete"])).drop_columns(["_is_delete", "_key"])
        deltalake.write_deltalake(target, rows, mode="overwrite", schema_mode="overwrite",
                                  storage_options=options)
        print(f"bronze.{table}: rebuilt from {rows.num_rows} events")
    elif events.num_rows:
        key = PRIMARY_KEYS[table]
        names = [*schema, "_ingested_at", "_cdc_ts"]
        updates = {name: f'source."{name}"' for name in names}
        (
            existing.merge(
                source=events,
                predicate=f'target."{key}" = source._key',
                source_alias="source",
                target_alias="target",
            )
            .when_matched_delete(predicate="source._is_delete")
            .when_matched_update(updates=updates, predicate="NOT source._is_delete")
            .when_not_matched_insert(updates=updates, predicate="NOT source._is_delete")
            .execute()
        )
        print(f"bronze.{table}: merged {events.num_rows} changes")
    else:
        print(f"bronze.{table}: up to date")

    from conn import vacuum_delta

    vacuum_delta(target, options)
