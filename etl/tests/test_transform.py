"""Regression tests for the silver -> gold -> mart Ibis transforms.

Each test pins a bug that was live in transform.py: stale table references that
survive a join, a dedup that dropped rows instead of selecting survivors, and a
status filter applied to the wrong relation.
"""
import pandas as pd


# --- silver: customers -----------------------------------------------------

def test_silver_customers_dedupes_on_normalized_email(built):
    df = built.df("silver", "customers")
    assert sorted(df["customer_id"]) == [1, 2]
    assert sorted(df["email"]) == ["alice@example.com", "bob@example.com"]


def test_silver_customers_cleanses_and_translates(built):
    df = built.df("silver", "customers").set_index("customer_id")
    assert df.loc[1, "full_name"] == "Alice Tan"
    assert df.loc[1, "country_code"] == "SG"
    # country_ref join must survive the lowercase source code
    assert df.loc[1, "country_name"] == "Singapore"


def test_silver_customers_keeps_email_column(built):
    # The dedup join previously projected email away entirely.
    assert "email" in built.df("silver", "customers").columns


# --- silver: products ------------------------------------------------------

def test_silver_products_dedupes_on_name_and_category(built):
    df = built.df("silver", "products")
    # products 2 and 7 collide on (product_name, category_code) -> keep min id
    assert sorted(df["product_id"]) == [1, 2]


def test_silver_products_drops_negative_price(built):
    assert 8 not in set(built.df("silver", "products")["product_id"])


def test_silver_products_keeps_surviving_row_values(built):
    df = built.df("silver", "products").set_index("product_id")
    assert df.loc[2, "product_name"] == "USB-C Cable"
    assert df.loc[2, "unit_price"] == 12.50
    assert df.loc[2, "category_name"] == "accessories"


# --- silver: transactions --------------------------------------------------

def test_silver_transactions_keeps_only_completed(built):
    df = built.df("silver", "transactions")
    assert sorted(df["transaction_id"]) == [1, 2]
    assert set(df["status"]) == {"completed"}


# --- silver: sales ---------------------------------------------------------

def test_silver_sales_computes_line_amount(built):
    df = built.df("silver", "sales").set_index("item_id")
    assert df.loc[1, "line_amount"] == 1200.00
    assert df.loc[2, "line_amount"] == 25.00   # 2 * 12.50
    assert df.loc[3, "line_amount"] == 37.50   # 3 * 12.50


def test_silver_sales_excludes_items_of_filtered_transactions(built):
    # item 4 hangs off pending transaction 3
    assert 4 not in set(built.df("silver", "sales")["item_id"])


def test_silver_sales_line_amount_matches_quantity_times_price(built):
    df = built.df("silver", "sales")
    expected = (df["quantity"] * df["unit_price"]).round(2)
    pd.testing.assert_series_equal(
        df["line_amount"].astype(float), expected.astype(float), check_names=False
    )


# --- gold ------------------------------------------------------------------

def test_gold_fact_sales_rowcount_matches_silver(built):
    assert len(built.df("gold", "fact_sales")) == len(built.df("silver", "sales"))


def test_gold_dim_date_is_distinct_and_decomposed(built):
    df = built.df("gold", "dim_date")
    assert df["date_key"].is_unique
    row = df[df["transaction_date"].astype(str) == "2026-08-01"].iloc[0]
    assert (row["year"], row["month"], row["day"]) == (2026, 8, 1)


def test_gold_fact_keys_resolve_against_dims(built):
    fact = built.df("gold", "fact_sales")
    for key, dim in [
        ("customer_key", "dim_customer"),
        ("product_key", "dim_product"),
        ("store_key", "dim_store"),
        ("date_key", "dim_date"),
    ]:
        dim_keys = set(built.df("gold", dim)[key])
        assert set(fact[key]) <= dim_keys, f"orphan {key} in fact_sales"


# --- mart ------------------------------------------------------------------

def test_daily_sales_mart_aggregates_revenue_per_store_day(built):
    df = built.df("mart", "daily_sales_mart")
    row = df[df["store_name"] == "Flagship Store"].iloc[0]
    assert row["revenue"] == 1225.00        # 1200.00 + 25.00
    assert row["units_sold"] == 3
    assert row["transaction_count"] == 1


def test_product_sales_mart_totals_match_fact(built):
    fact_revenue = built.df("gold", "fact_sales")["line_amount"].sum()
    assert built.df("mart", "product_sales_mart")["revenue"].sum() == fact_revenue


def test_customer_sales_mart_totals_match_fact(built):
    fact_revenue = built.df("gold", "fact_sales")["line_amount"].sum()
    mart = built.df("mart", "customer_sales_mart")
    assert mart["total_spent"].sum() == fact_revenue
    assert "last_sale_date" in mart.columns


def test_marts_have_no_duplicate_grain(built):
    daily = built.df("mart", "daily_sales_mart")
    assert not daily.duplicated(subset=["date_key", "store_name"]).any()
    cust = built.df("mart", "customer_sales_mart")
    assert not cust.duplicated(subset=["customer_key"]).any()
