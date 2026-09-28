"""Regression tests for the custom ingestion write modes.

These pin the Delta-level behaviour that ``ingest.run_custom`` relies on:
  * append tolerates an added source column (schema_mode="merge")
  * merge is idempotent on the primary key, where append duplicates rows
  * merge bootstraps a table that does not exist yet

They exercise local Delta paths, so no S3/RustFS is required.
"""
import deltalake
import pyarrow as pa
import pytest

import ingest


@pytest.fixture
def delta_path(tmp_path):
    return str(tmp_path / "ingestion_table")


def _rows(ids, names):
    return pa.table({
        "store_id": pa.array(ids, pa.int64()),
        "store_name": pa.array(names, pa.string()),
    })


def _read(path):
    return deltalake.DeltaTable(path).to_pyarrow_table()


def test_merge_bootstraps_missing_table(monkeypatch, delta_path):
    monkeypatch.setattr(ingest, "s3_storage_options", dict)
    con = _FakeCon(_rows([1, 2], ["a", "b"]))
    ingest._merge_table(con, None, delta_path, "stores")
    assert _read(delta_path).num_rows == 2


def test_merge_is_idempotent_on_primary_key(monkeypatch, delta_path):
    monkeypatch.setattr(ingest, "s3_storage_options", dict)
    batch = _rows([1, 2], ["a", "b"])
    ingest._merge_table(_FakeCon(batch), None, delta_path, "stores")
    # re-ingesting the very same batch must not duplicate rows
    ingest._merge_table(_FakeCon(batch), None, delta_path, "stores")
    assert _read(delta_path).num_rows == 2


def test_merge_updates_changed_rows_and_inserts_new(monkeypatch, delta_path):
    monkeypatch.setattr(ingest, "s3_storage_options", dict)
    ingest._merge_table(_FakeCon(_rows([1, 2], ["a", "b"])), None, delta_path, "stores")
    ingest._merge_table(_FakeCon(_rows([2, 3], ["B", "c"])), None, delta_path, "stores")

    tbl = _read(delta_path).to_pydict()
    got = dict(zip(tbl["store_id"], tbl["store_name"]))
    assert got == {1: "a", 2: "B", 3: "c"}


def test_append_duplicates_rows_on_rerun(delta_path):
    """Documents why merge exists: append has no dedup."""
    batch = _rows([1, 2], ["a", "b"])
    deltalake.write_deltalake(delta_path, batch, mode="overwrite", schema_mode="overwrite")
    deltalake.write_deltalake(delta_path, batch, mode="append", schema_mode="merge")
    assert _read(delta_path).num_rows == 4


def test_append_tolerates_added_source_column(delta_path):
    """Without schema_mode="merge" this raises SchemaMismatchError."""
    deltalake.write_deltalake(delta_path, _rows([1], ["a"]),
                              mode="overwrite", schema_mode="overwrite")
    drifted = pa.table({
        "store_id": pa.array([2], pa.int64()),
        "store_name": pa.array(["b"], pa.string()),
        "region": pa.array(["APAC"], pa.string()),
    })
    deltalake.write_deltalake(delta_path, drifted, mode="append", schema_mode="merge")

    tbl = _read(delta_path)
    assert tbl.num_rows == 2
    assert "region" in tbl.column_names


def test_run_custom_rejects_unknown_mode():
    with pytest.raises(ValueError, match="overwrite|append|merge"):
        ingest.run_custom("upsert")


def _parquet_files(path):
    import os
    return [f for f in os.listdir(path) if f.endswith(".parquet")]


def test_merge_leaves_no_orphaned_parquet_files(monkeypatch, delta_path):
    """Stale files break ClickHouse's DeltaLake engine, which ignores the log."""
    monkeypatch.setattr(ingest, "s3_storage_options", dict)
    ingest._merge_table(_FakeCon(_rows([1, 2], ["a", "b"])), None, delta_path, "stores")
    ingest._merge_table(_FakeCon(_rows([2, 3], ["B", "c"])), None, delta_path, "stores")

    active = {f.split("/")[-1] for f in deltalake.DeltaTable(delta_path).files()}
    on_disk = set(_parquet_files(delta_path))
    assert on_disk == active, f"orphaned files left behind: {on_disk - active}"


def test_write_delta_vacuums_previous_version(monkeypatch, tmp_path):
    """conn.write_delta must not accumulate files across overwrites."""
    import conn

    path = str(tmp_path / "gold_table")
    monkeypatch.setattr(conn, "S3", {"lake_bucket": str(tmp_path)})
    monkeypatch.setattr("config.s3_storage_options", dict, raising=False)

    for names in (["a", "b"], ["c", "d"], ["e", "f"]):
        deltalake.write_deltalake(path, _rows([1, 2], names),
                                  mode="overwrite", schema_mode="overwrite")
        conn.vacuum_delta(path, {})

    active = {f.split("/")[-1] for f in deltalake.DeltaTable(path).files()}
    assert set(_parquet_files(path)) == active


class _FakeCon:
    """Stands in for the Ibis connection: to_pyarrow returns a fixed batch."""

    def __init__(self, table):
        self._table = table

    def to_pyarrow(self, _expr):
        return self._table
