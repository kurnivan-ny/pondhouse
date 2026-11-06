"""Delta maintenance in conn.py, on local paths, so no S3/RustFS is required."""
import os

import deltalake
import pyarrow as pa

import conn


def _rows(ids, names):
    return pa.table({
        "store_id": pa.array(ids, pa.int64()),
        "store_name": pa.array(names, pa.string()),
    })


def test_vacuum_delta_removes_previous_versions(tmp_path):
    """Repeated overwrites must not accumulate parquet files."""
    path = str(tmp_path / "gold_table")

    for names in (["a", "b"], ["c", "d"], ["e", "f"]):
        deltalake.write_deltalake(path, _rows([1, 2], names),
                                  mode="overwrite", schema_mode="overwrite")
        conn.vacuum_delta(path, {})

    active = {f.split("/")[-1] for f in deltalake.DeltaTable(path).file_uris()}
    assert {f for f in os.listdir(path) if f.endswith(".parquet")} == active
