"""Ibis transformations: bronze -> silver -> gold -> mart (all Delta on RustFS).

Retail medallion:
  silver — translate (join reference lookups), cleanse, deduplicate
  gold   — star schema (dim_* + fact_sales)
  mart   — business-ready data marts

Each output table has one builder returning an Ibis expression, listed in TRANSFORMS
with the tables it reads. Dagster makes one asset per entry; `run_transform` runs
them in order.

Usage:
  python transform.py
"""
from typing import Callable, NamedTuple

import ibis
from ibis import _

from config import S3
from conn import get_conn, read_delta, write_delta


UNKNOWN = "Unknown"


def _read(con, layer, table):
    path = f"s3://{S3['lake_bucket']}/{layer}/{table}"
    return read_delta(con, path, f"{layer}_{table}")


def _dedupe(table, key):
    """Keep one row per primary key, so duplicated source rows cannot fan out joins."""
    window = ibis.window(group_by=key, order_by=key)
    return table.mutate(_rn=ibis.row_number().over(window)).filter(_._rn == 0).drop("_rn")


def _read_bronze(con, table, key):
    return _dedupe(_read(con, "bronze", table), key)


# ---------------------------------------------------------------------------
# SILVER: translate + cleanse + deduplicate
# ---------------------------------------------------------------------------
def _clean_customers(con):
    customers = _read_bronze(con, "customers", "customer_id")
    return customers.mutate(
        full_name=customers.full_name.strip(),
        email=customers.email.lower(),
        country_code=customers.country_code.upper(),
    )


def _surviving_customers(clean):
    """The lowest customer_id of each email: the id that survives dedup."""
    return clean.group_by("email").agg(customer_id=clean.customer_id.min())


def _clean_products(con):
    products = _read_bronze(con, "products", "product_id")
    return products.mutate(
        product_name=products.product_name.strip(),
        category_code=products.category_code.upper(),
    ).filter(products.unit_price >= 0)


def _surviving_products(clean):
    """The lowest product_id of each (name, category): the id that survives dedup."""
    return clean.group_by(["product_name", "category_code"]).agg(
        product_id=clean.product_id.min()
    )


def silver_customers(con):
    # An unknown country code keeps the customer, labelled "Unknown", so their sales stay.
    clean = _clean_customers(con)
    country_ref = _read_bronze(con, "country_ref", "country_code")
    return (
        _surviving_customers(clean)
        .join(
            clean.select("customer_id", "full_name", "country_code", "signup_date"),
            "customer_id",
        )
        .left_join(country_ref, "country_code")
        .mutate(country_name=ibis.coalesce(_.country_name, UNKNOWN))
        .select(
            "customer_id",
            "full_name",
            "email",
            "country_code",
            "country_name",
            "signup_date",
        )
    )


def silver_products(con):
    clean = _clean_products(con)
    category_ref = _read_bronze(con, "category_ref", "category_code")
    return (
        _surviving_products(clean)
        .join(clean.select("product_id", "unit_price"), "product_id")
        .left_join(category_ref, "category_code")
        .mutate(category_name=ibis.coalesce(_.category_name, UNKNOWN))
        .select("product_id", "product_name", "category_code", "category_name", "unit_price")
    )


def silver_stores(con):
    stores = _read_bronze(con, "stores", "store_id")
    country_ref = _read_bronze(con, "country_ref", "country_code")
    return (
        stores.mutate(
            store_name=stores.store_name.strip(),
            city=stores.city.strip(),
            country_code=stores.country_code.upper(),
        )
        .left_join(country_ref, "country_code")
        .mutate(country_name=ibis.coalesce(_.country_name, UNKNOWN))
        .select("store_id", "store_name", "city", "country_code", "country_name")
    )


def silver_transactions(con):
    """Completed transactions, following the surviving customer."""
    transactions = _read_bronze(con, "transactions", "transaction_id")
    status_ref = _read_bronze(con, "status_ref", "status_code")
    clean = _clean_customers(con)
    # Every source id -> the id that survived dedup, so sales follow the survivor.
    customer_map = clean.select("customer_id", "email").join(
        _surviving_customers(clean).rename(surviving_customer_id="customer_id"), "email"
    ).select("customer_id", "surviving_customer_id")
    return (
        transactions.join(status_ref, "status_code")
        .mutate(status=_.status_name.lower())
        .filter(_.status == "completed")
        .join(customer_map, "customer_id")
        .mutate(customer_id=_.surviving_customer_id)
        .select(
            "transaction_id",
            "store_id",
            "customer_id",
            "transaction_date",
            "status",
            "payment_method",
        )
    )


def _completed_items(con):
    items = _read_bronze(con, "transaction_items", "item_id").select(
        "item_id", "transaction_id", "product_id", "quantity", "unit_price"
    )
    return items.join(_read(con, "silver", "transactions"), "transaction_id")


def silver_sales(con):
    """Items of completed transactions, following the surviving product, joined to
    valid dims, with line_amount computed."""
    clean = _clean_products(con)
    product_map = clean.select("product_id", "product_name", "category_code").join(
        _surviving_products(clean).rename(surviving_product_id="product_id"),
        ["product_name", "category_code"],
    ).select("product_id", "surviving_product_id")
    return (
        _completed_items(con)
        .join(product_map, "product_id")
        .mutate(
            product_id=_.surviving_product_id,
            line_amount=(_.quantity * _.unit_price).round(2),
        )
        .semi_join(_read(con, "silver", "customers"), "customer_id")
        .semi_join(_read(con, "silver", "stores"), "store_id")
        .select(
            "item_id",
            "transaction_id",
            "store_id",
            "customer_id",
            "product_id",
            "transaction_date",
            "status",
            "payment_method",
            "quantity",
            "unit_price",
            "line_amount",
        )
    )


def silver_rejected_sales(con):
    """Completed items that could not become a sale (e.g. an invalid product).

    Kept for inspection, so sales + rejected always reconciles with the source.
    """
    return _completed_items(con).anti_join(_read(con, "silver", "sales"), "item_id").select(
        "item_id", "transaction_id", "store_id", "customer_id", "product_id",
        "quantity", "unit_price",
    )


# ---------------------------------------------------------------------------
# GOLD: star schema (dimensions + fact)
# ---------------------------------------------------------------------------
def gold_dim_customer(con):
    customers = _read(con, "silver", "customers")
    return customers.select(
        customer_key=customers.customer_id,
        customer_id=customers.customer_id,
        full_name=customers.full_name,
        email=customers.email,
        country=customers.country_name,
        signup_date=customers.signup_date,
    )


def gold_dim_product(con):
    products = _read(con, "silver", "products")
    return products.select(
        product_key=products.product_id,
        product_id=products.product_id,
        product_name=products.product_name,
        category=products.category_name,
        unit_price=products.unit_price,
    )


def gold_dim_store(con):
    stores = _read(con, "silver", "stores")
    return stores.select(
        store_key=stores.store_id,
        store_id=stores.store_id,
        store_name=stores.store_name,
        city=stores.city,
        country=stores.country_name,
    )


def gold_dim_date(con):
    dates = _read(con, "silver", "sales").select("transaction_date").distinct()
    return (
        dates.mutate(
            date_key=dates.transaction_date,
            year=dates.transaction_date.year(),
            month=dates.transaction_date.month(),
            day=dates.transaction_date.day(),
        )
        .select("date_key", "transaction_date", "year", "month", "day")
        .order_by("transaction_date")
    )


def gold_fact_sales(con):
    sales = _read(con, "silver", "sales")
    return sales.select(
        item_key=sales.item_id,
        date_key=sales.transaction_date,
        customer_key=sales.customer_id,
        product_key=sales.product_id,
        store_key=sales.store_id,
        transaction_id=sales.transaction_id,
        quantity=sales.quantity,
        unit_price=sales.unit_price,
        line_amount=sales.line_amount,
    )


# ---------------------------------------------------------------------------
# MART: business-ready data marts
# ---------------------------------------------------------------------------
def mart_daily_sales(con):
    """Daily sales by store."""
    sales = _read(con, "gold", "fact_sales")
    return (
        sales.join(_read(con, "gold", "dim_store"), "store_key")
        .group_by(["date_key", "store_name"])
        .agg(
            transaction_count=sales.transaction_id.nunique(),
            units_sold=sales.quantity.sum(),
            revenue=sales.line_amount.sum(),
        )
        .order_by(["date_key", "store_name"])
    )


def mart_product_sales(con):
    """Product and category performance."""
    sales = _read(con, "gold", "fact_sales")
    return (
        sales.join(_read(con, "gold", "dim_product"), "product_key")
        .group_by(["category", "product_name"])
        .agg(
            units_sold=sales.quantity.sum(),
            revenue=sales.line_amount.sum(),
        )
        .order_by(ibis.desc("revenue"))
    )


def mart_customer_sales(con):
    """Customer value."""
    sales = _read(con, "gold", "fact_sales")
    return (
        sales.join(_read(con, "gold", "dim_customer"), "customer_key")
        .group_by(["customer_key", "full_name", "country"])
        .agg(
            transaction_count=sales.transaction_id.nunique(),
            total_spent=sales.line_amount.sum(),
            last_sale_date=sales.date_key.max(),
        )
        .order_by(ibis.desc("total_spent"))
    )


# ---------------------------------------------------------------------------
# Every table, in build order, with the lake tables it reads
# ---------------------------------------------------------------------------
class Transform(NamedTuple):
    layer: str
    table: str
    build: Callable
    deps: tuple


def _bronze(*tables):
    return tuple(("bronze", t) for t in tables)


def _silver(*tables):
    return tuple(("silver", t) for t in tables)


def _gold(*tables):
    return tuple(("gold", t) for t in tables)


TRANSFORMS = [
    Transform("silver", "customers", silver_customers, _bronze("customers", "country_ref")),
    Transform("silver", "products", silver_products, _bronze("products", "category_ref")),
    Transform("silver", "stores", silver_stores, _bronze("stores", "country_ref")),
    Transform("silver", "transactions", silver_transactions,
              _bronze("transactions", "status_ref", "customers")),
    Transform("silver", "sales", silver_sales,
              _bronze("transaction_items", "products")
              + _silver("transactions", "customers", "stores")),
    Transform("silver", "rejected_sales", silver_rejected_sales,
              _bronze("transaction_items") + _silver("transactions", "sales")),
    Transform("gold", "dim_customer", gold_dim_customer, _silver("customers")),
    Transform("gold", "dim_product", gold_dim_product, _silver("products")),
    Transform("gold", "dim_store", gold_dim_store, _silver("stores")),
    Transform("gold", "dim_date", gold_dim_date, _silver("sales")),
    Transform("gold", "fact_sales", gold_fact_sales, _silver("sales")),
    Transform("mart", "daily_sales_mart", mart_daily_sales, _gold("fact_sales", "dim_store")),
    Transform("mart", "product_sales_mart", mart_product_sales,
              _gold("fact_sales", "dim_product")),
    Transform("mart", "customer_sales_mart", mart_customer_sales,
              _gold("fact_sales", "dim_customer")),
]


def build_table(con, transform):
    write_delta(con, transform.build(con), transform.layer, transform.table)


def _build_layer(con, layer):
    for transform in TRANSFORMS:
        if transform.layer == layer:
            build_table(con, transform)


def build_silver(con):
    _build_layer(con, "silver")


def build_gold(con):
    _build_layer(con, "gold")


def build_mart(con):
    _build_layer(con, "mart")


def run_transform():
    con = get_conn()
    try:
        build_silver(con)
        build_gold(con)
        build_mart(con)
    finally:
        try:
            con.disconnect()
        except Exception:
            pass


if __name__ == "__main__":
    run_transform()
