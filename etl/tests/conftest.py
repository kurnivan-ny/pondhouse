"""Shared fixtures: in-memory medallion source data for the transform tests.

The transform functions in ``transform.py`` read via ``_read(con, layer, table)``
and write via ``write_delta``. Tests monkeypatch those two seams so the whole
silver/gold/mart pipeline runs against in-memory tables on a real engine
(DuckDB by default, ClickHouse when it is reachable) with no S3/Delta involved.
"""
import os
import sys
from datetime import date

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import ibis  # noqa: E402


# --- Source fixtures -------------------------------------------------------
# Deliberately dirty: whitespace/case noise, a duplicate email, a duplicate
# (product_name, category_code), a negative price, and non-completed statuses.

BRONZE = {
    "customers": [
        # customer_id, full_name, email, country_code, signup_date
        (1, "  Alice Tan  ", "ALICE@example.com", "sg", "2026-01-15"),
        (2, "Bob Lim", "bob@example.com", "MY", "2026-02-01"),
        # duplicate of customer 1 by email (differs in case/whitespace)
        (5, "Alice Tan", "alice@example.com", "SG", "2026-04-01"),
    ],
    "products": [
        # product_id, product_name, category_code, unit_price
        (1, "Laptop 14\"", "ELEC", 1200.00),
        (2, " USB-C Cable ", "access", 12.50),
        # duplicate of product 2 by (product_name, category_code)
        (7, "USB-C Cable", "ACCESS", 13.00),
        # invalid price -> must be cleansed away
        (8, "Broken Item", "ELEC", -1.00),
    ],
    "stores": [
        (1, " Flagship Store ", " Singapore ", "sg"),
        (2, "Mall Kiosk", "Kuala Lumpur", "MY"),
    ],
    "transactions": [
        # transaction_id, store_id, customer_id, transaction_date, status_code, payment_method
        (1, 1, 1, "2026-08-01", "C", "CARD"),
        (2, 2, 2, "2026-08-02", "C", "CASH"),
        # non-completed -> must be filtered out of silver
        (3, 1, 1, "2026-08-03", "P", "EWALLET"),
        (4, 2, 2, "2026-08-04", "X", "CASH"),
    ],
    "transaction_items": [
        # item_id, transaction_id, product_id, quantity, unit_price
        (1, 1, 1, 1, 1200.00),
        (2, 1, 2, 2, 12.50),
        (3, 2, 2, 3, 12.50),
        # belongs to a pending transaction -> filtered out with transaction 3
        (4, 3, 1, 1, 1200.00),
    ],
    "category_ref": [("ELEC", "electronics"), ("ACCESS", "accessories")],
    "status_ref": [("C", "completed"), ("P", "pending"), ("X", "cancelled")],
    "country_ref": [("SG", "Singapore"), ("MY", "Malaysia")],
}

SCHEMAS = {
    "customers": dict(
        customer_id="int64", full_name="string", email="string",
        country_code="string", signup_date="date",
    ),
    "products": dict(
        product_id="int64", product_name="string",
        category_code="string", unit_price="float64",
    ),
    "stores": dict(
        store_id="int64", store_name="string", city="string", country_code="string",
    ),
    "transactions": dict(
        transaction_id="int64", store_id="int64", customer_id="int64",
        transaction_date="date", status_code="string", payment_method="string",
    ),
    "transaction_items": dict(
        item_id="int64", transaction_id="int64", product_id="int64",
        quantity="int64", unit_price="float64",
    ),
    "category_ref": dict(category_code="string", category_name="string"),
    "status_ref": dict(status_code="string", status_name="string"),
    "country_ref": dict(country_code="string", country_name="string"),
}


def _rows_as_dicts(table):
    """Rows as dicts, with date-typed columns parsed into real date objects.

    ClickHouse's Arrow-based loader will not coerce ISO strings to dates the way
    DuckDB does, so the fixture has to hand both backends the same Python types.
    """
    cols = list(SCHEMAS[table])
    date_cols = [c for c, t in SCHEMAS[table].items() if t == "date"]
    rows = []
    for row in BRONZE[table]:
        rec = dict(zip(cols, row))
        for c in date_cols:
            rec[c] = date.fromisoformat(rec[c])
        rows.append(rec)
    return rows


class Lake:
    """In-memory stand-in for the Delta lake: layer/table -> ibis table."""

    def __init__(self, con):
        self.con = con
        self.tables = {}

    def seed_bronze(self):
        for table in BRONZE:
            name = f"bronze_{table}"
            self.tables[("bronze", table)] = self.con.create_table(
                name,
                _rows_as_dicts(table),
                schema=ibis.schema(SCHEMAS[table]),
                overwrite=True,
            )

    def read(self, _con, layer, table):
        return self.tables[(layer, table)]

    def write(self, con, expr, layer, table):
        # Materialize exactly like write_delta does, so an expression that only
        # fails at execution time (stale table refs, missing columns) fails here.
        name = f"{layer}_{table}"
        self.tables[(layer, table)] = con.create_table(name, expr, overwrite=True)

    def df(self, layer, table):
        return self.tables[(layer, table)].to_pandas()


def _make_lake(con, monkeypatch):
    import transform

    lake = Lake(con)
    lake.seed_bronze()
    monkeypatch.setattr(transform, "_read", lake.read)
    monkeypatch.setattr(transform, "write_delta", lake.write)
    return lake


@pytest.fixture
def duckdb_con():
    con = ibis.duckdb.connect()
    yield con
    con.disconnect()


@pytest.fixture
def lake(duckdb_con, monkeypatch):
    """DuckDB-backed lake with bronze seeded and transform.py rewired to it."""
    return _make_lake(duckdb_con, monkeypatch)


@pytest.fixture
def built(lake):
    """Full silver -> gold -> mart build on DuckDB."""
    import transform

    transform.build_silver(lake.con)
    transform.build_gold(lake.con)
    transform.build_mart(lake.con)
    return lake


# --- ClickHouse (backend-compatibility) ------------------------------------

def _clickhouse_con():
    return ibis.clickhouse.connect(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        user=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "pondhouse"),
        database=os.getenv("CLICKHOUSE_TEST_DB", "default"),
    )


@pytest.fixture
def clickhouse_con():
    pytest.importorskip("clickhouse_connect")
    try:
        con = _clickhouse_con()
        con.raw_sql("SELECT 1")
    except Exception as e:  # pragma: no cover - env dependent
        pytest.skip(f"ClickHouse not reachable: {e}")
    yield con
    try:
        con.disconnect()
    except Exception:
        pass


@pytest.fixture
def ch_lake(clickhouse_con, monkeypatch):
    return _make_lake(clickhouse_con, monkeypatch)
