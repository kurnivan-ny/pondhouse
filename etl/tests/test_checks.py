"""The data-quality checks must pass on good data and fail on bad data."""
import checks
import serve


def _reader(lake):
    return lambda layer, table: lake.tables[(layer, table)]


def test_row_counts_pass_on_seeded_bronze(built):
    result = checks.check_row_counts(_reader(built), "bronze")
    assert result["passed"]
    assert result["metadata"]["row_counts"]["customers"] == 4


def test_row_counts_fail_on_an_empty_table(built):
    con = built.con
    built.tables[("bronze", "stores")] = con.create_table(
        "empty_stores", built.tables[("bronze", "stores")].limit(0)
    )
    result = checks.check_row_counts(_reader(built), "bronze")
    assert not result["passed"]
    assert result["metadata"]["empty_tables"] == ["stores"]


def test_gold_passes_and_reports_rejections(built):
    result = checks.check_gold(_reader(built))
    assert result["passed"]
    assert result["metadata"]["rejected_sales"] == 1


def test_gold_fails_on_duplicate_fact_rows(built):
    fact = built.tables[("gold", "fact_sales")]
    built.tables[("gold", "fact_sales")] = built.con.create_table(
        "doubled", fact.union(fact)
    )
    result = checks.check_gold(_reader(built))
    assert not result["passed"]
    assert result["metadata"]["duplicate_item_keys"] > 0


def test_gold_fails_on_orphan_key(built):
    dim = built.tables[("gold", "dim_store")]
    built.tables[("gold", "dim_store")] = built.con.create_table(
        "one_store", dim.filter(dim.store_key == 1)
    )
    result = checks.check_gold(_reader(built))
    assert not result["passed"]
    assert result["metadata"]["orphan_keys"]["store_key"] > 0


def test_gold_fails_when_revenue_does_not_reconcile(built):
    sales = built.tables[("silver", "sales")]
    built.tables[("silver", "sales")] = built.con.create_table(
        "less_sales", sales.filter(sales.item_id != 1)
    )
    assert not checks.check_gold(_reader(built))["passed"]


def test_marts_pass_when_counts_match(built):
    read = _reader(built)
    counts = {t: checks._count(read(layer, t)) for t, (layer, _key) in serve.MARTS.items()}
    assert checks.check_marts(read, counts.__getitem__)["passed"]


def test_marts_fail_when_clickhouse_is_behind(built):
    result = checks.check_marts(_reader(built), lambda table: 0)
    assert not result["passed"]
    assert "fact_sales" in result["metadata"]["mismatched"]


def test_table_rows_fail_on_one_empty_table(built):
    empty = built.tables[("bronze", "stores")].limit(0)
    assert not checks.check_table_rows(lambda _l, _t: empty, "bronze", "stores")["passed"]
    assert checks.check_table_rows(_reader(built), "bronze", "stores")["passed"]


def test_served_table_compares_one_table(built):
    read = _reader(built)
    rows = checks._count(read("gold", "fact_sales"))
    assert checks.check_served_table(read, "fact_sales", lambda _t: rows)["passed"]
    assert not checks.check_served_table(read, "fact_sales", lambda _t: rows - 1)["passed"]
