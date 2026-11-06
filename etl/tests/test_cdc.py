"""Merging Debezium events into bronze, on local files shaped like the real landing data."""
import datetime

import deltalake
import duckdb
import ibis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import cdc

SCHEMA = {"customer_id": "int32", "full_name": "string", "signup_date": "date"}
AFTER = pa.struct([("customer_id", pa.int32()), ("full_name", pa.string()),
                   ("signup_date", pa.int32())])
VALUE = pa.struct([("before", AFTER), ("after", AFTER), ("op", pa.string()), ("ts_ns", pa.int64())])
DAYS = (datetime.date(2026, 1, 15) - datetime.date(1970, 1, 1)).days


def _row(customer_id, name):
    return {"customer_id": customer_id, "full_name": name, "signup_date": DAYS}


def _event(op, ts, after=None, before=None):
    return {"value": {"before": before, "after": after, "op": op, "ts_ns": ts}}


class Landing:
    def __init__(self, tmp_path):
        self.dir = tmp_path / "landing" / "0"
        self.dir.mkdir(parents=True)
        self.target = str(tmp_path / "bronze_customers")
        self.count = 0

    def write(self, *events):
        table = pa.Table.from_pylist(list(events), schema=pa.schema([("value", VALUE)]))
        pq.write_table(table, self.dir / f"{self.count:020}.parquet")
        self.count += 1

    def apply(self, target=None):
        cdc.apply_events(ibis.duckdb.connect(), "customers", SCHEMA, target=target or self.target,
                         landing=str(self.dir / "*.parquet"), storage_options={})

    def rows(self):
        table = deltalake.DeltaTable(self.target).to_pyarrow_table()
        return {r["customer_id"]: r["full_name"] for r in table.to_pylist()}


@pytest.fixture
def landing(tmp_path):
    return Landing(tmp_path)


def test_debezium_snapshot_builds_the_table_with_real_dates(landing):
    landing.write(_event("r", 1, _row(1, "Alice")), _event("r", 2, _row(2, "Bob")))
    landing.apply()
    table = deltalake.DeltaTable(landing.target).to_pyarrow_table()
    assert landing.rows() == {1: "Alice", 2: "Bob"}
    assert table.to_pylist()[0]["signup_date"] == datetime.date(2026, 1, 15)
    assert not {"_is_delete", "_key"} & set(table.column_names)


def test_update_insert_and_delete_are_merged(landing):
    landing.write(_event("r", 1, _row(1, "Alice")), _event("r", 2, _row(2, "Bob")))
    landing.apply()
    landing.write(
        _event("u", 3, _row(1, "Alice Tan"), _row(1, "Alice")),
        _event("c", 4, _row(3, "Carol")),
        _event("d", 5, None, {"customer_id": 2, "full_name": None, "signup_date": None}),
    )
    landing.apply()
    assert landing.rows() == {1: "Alice Tan", 3: "Carol"}


def test_only_the_newest_event_per_key_counts(landing):
    landing.write(_event("r", 1, _row(1, "Alice")))
    landing.apply()
    landing.write(_event("u", 2, _row(1, "Second")), _event("u", 3, _row(1, "Third")))
    landing.apply()
    assert landing.rows() == {1: "Third"}


def test_insert_then_delete_in_one_batch_leaves_nothing(landing):
    landing.write(_event("r", 1, _row(1, "Alice")))
    landing.apply()
    landing.write(
        _event("c", 2, _row(2, "Brief")),
        _event("d", 3, None, {"customer_id": 2, "full_name": None, "signup_date": None}),
    )
    landing.apply()
    assert landing.rows() == {1: "Alice"}


def test_applying_again_changes_nothing(landing):
    landing.write(_event("r", 1, _row(1, "Alice")), _event("r", 2, _row(2, "Bob")))
    landing.apply()
    landing.write(_event("d", 3, None, {"customer_id": 2, "full_name": None, "signup_date": None}))
    landing.apply()
    version = deltalake.DeltaTable(landing.target).version()
    landing.apply()
    assert landing.rows() == {1: "Alice"}
    # The delete event is newer than every remaining row, so it is reapplied harmlessly.
    assert deltalake.DeltaTable(landing.target).version() >= version


def test_only_new_events_are_read(landing):
    landing.write(_event("r", 1, _row(1, "Alice")))
    landing.apply()
    sql = cdc.latest_events_sql("customers", SCHEMA, str(landing.dir / "*.parquet"), 1)
    assert duckdb.sql(sql).fetchall() == []


def test_a_table_from_snapshot_mode_is_replaced_from_the_full_topic(landing):
    deltalake.write_deltalake(landing.target, pa.table({
        "customer_id": pa.array([9], pa.int32()), "full_name": ["from snapshot mode"],
    }))
    landing.write(_event("r", 1, _row(1, "Alice")), _event("r", 2, _row(2, "Bob")),
                  _event("d", 3, None, {"customer_id": 2, "full_name": None, "signup_date": None}))
    landing.apply()
    assert landing.rows() == {1: "Alice"}


def test_a_table_with_no_events_exists_but_is_empty(landing):
    landing.apply()
    assert deltalake.DeltaTable(landing.target).to_pyarrow_table().num_rows == 0
    assert landing.rows() == {}


def test_every_source_table_has_a_primary_key():
    import bronze

    assert set(cdc.PRIMARY_KEYS) == set(bronze.SOURCE_SCHEMAS)
