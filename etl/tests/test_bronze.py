"""Bronze fixes one schema, so the ingestion path cannot leak into later layers."""
import ibis

import bronze
from config import TABLES


def test_every_source_table_has_a_schema():
    assert set(bronze.SOURCE_SCHEMAS) == set(TABLES)


def test_cast_normalizes_sling_decimal_and_keeps_extra_columns():
    con = ibis.duckdb.connect()
    con.raw_sql(
        "CREATE TABLE landed AS SELECT 1::BIGINT AS product_id, 'x' AS product_name, "
        "'ELEC' AS category_code, 12.5::DECIMAL(24, 6) AS unit_price, "
        "now() AS _loaded_at"
    )
    result = con.table("landed").cast(bronze.SOURCE_SCHEMAS["products"])

    assert result.schema()["unit_price"] == ibis.expr.datatypes.Decimal(10, 2)
    assert result.schema()["product_id"] == ibis.expr.datatypes.Int32()
    assert "_loaded_at" in result.columns
    assert float(result.unit_price.sum().execute()) == 12.5
