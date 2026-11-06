"""Ibis transformations: bronze -> silver -> gold -> mart (all Delta on RustFS).

Retail medallion:
  silver — translate (join reference lookups), cleanse, deduplicate
  gold   — star schema (dim_* + fact_sales)
  mart   — business-ready data marts

Each output table has one builder returning an Ibis expression, listed in TRANSFORMS
with the tables it reads. Dagster makes one asset per entry; `run_transform` runs
them in order.

Every table can be rebuilt from bronze, except silver.customer_history. It is the only
record of what customers used to look like, because the source only holds today's values.

Usage:
  python transform.py
"""
import calendar
from datetime import date, timedelta
from typing import Callable, NamedTuple

import ibis
from ibis import _

from config import S3, s3_storage_options
from conn import get_conn, read_delta, write_delta


UNKNOWN = "Unknown"

# A customer's first version starts here, so sales dated before the first capture still
# find a version, and their current version ends at FOREVER. Both must fit ClickHouse's
# DateTime, which runs from 1970 to 2106 and silently clamps anything outside.
BEGINNING = "1970-01-01 00:00:00"
FOREVER = "2100-01-01 00:00:00"

# customer_key = customer_id * this + version. Deterministic, so a rebuild gives the same
# keys, and works the same on every engine.
CUSTOMER_KEY_FACTOR = 1_000_000

# The columns whose change starts a new customer version.
TRACKED_CUSTOMER_COLUMNS = [
    "customer_id", "full_name", "email", "country_code", "country_name", "signup_date",
]


def _read(con, layer, table):
    path = f"s3://{S3['lake_bucket']}/{layer}/{table}"
    return read_delta(con, path, f"{layer}_{table}")


def _exists(con, layer, table):
    import deltalake

    path = f"s3://{S3['lake_bucket']}/{layer}/{table}"
    return deltalake.DeltaTable.is_deltatable(path, storage_options=s3_storage_options())


def _now():
    """When this run observed the data. A seam, so tests can control time."""
    return ibis.now().cast("timestamp")


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


def silver_customer_history(con):
    """Every version of every customer, as observed by this pipeline.

    Each run adds a row only for customers that changed, appeared or disappeared, so
    re-running with unchanged data adds nothing. A disappeared customer gets a row with
    is_deleted set, which closes their last version.
    """
    current = _read(con, "silver", "customers").select(*TRACKED_CUSTOMER_COLUMNS)
    beginning = ibis.timestamp(BEGINNING)
    if not _exists(con, "silver", "customer_history"):
        return current.mutate(valid_from=beginning, is_deleted=ibis.literal(False))

    history = _read(con, "silver", "customer_history")
    window = ibis.window(group_by="customer_id", order_by=ibis.desc("valid_from"))
    latest = history.mutate(_rn=ibis.row_number().over(window)).filter(_._rn == 0).drop("_rn")
    active = latest.filter(~latest.is_deleted)

    changed = current.anti_join(active, TRACKED_CUSTOMER_COLUMNS)
    # A new customer's first version also starts at the beginning: their sales can be
    # dated before this run observed them.
    known = history.select("customer_id").distinct().mutate(_known=ibis.literal(True))
    versions = (
        changed.left_join(known, "customer_id")
        .mutate(
            valid_from=ibis.ifelse(_._known.notnull(), _now(), beginning),
            is_deleted=ibis.literal(False),
        )
        .select(*TRACKED_CUSTOMER_COLUMNS, "valid_from", "is_deleted")
    )
    removed = (
        active.anti_join(current, "customer_id")
        .mutate(valid_from=_now(), is_deleted=ibis.literal(True))
        .select(*TRACKED_CUSTOMER_COLUMNS, "valid_from", "is_deleted")
    )
    columns = [*TRACKED_CUSTOMER_COLUMNS, "valid_from", "is_deleted"]
    existing = history.select(*columns)
    return existing.union(versions.cast(existing.schema())).union(removed.cast(existing.schema()))


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


def _checked_refunds(con):
    """Refunds of known sales, with the running total refunded for each item."""
    refunds = _read_bronze(con, "refunds", "refund_id").select(
        "refund_id", "item_id", "refund_date", "quantity", "amount", "reason",
    )
    sales = _read(con, "silver", "sales").select(
        "item_id", "transaction_id", "store_id", "customer_id", "product_id", "line_amount",
    )
    window = ibis.window(group_by="item_id", order_by=["refund_date", "refund_id"],
                         preceding=None, following=0)
    return refunds.join(sales, "item_id").mutate(refunded_so_far=_.amount.sum().over(window))


def silver_refunds(con):
    """Valid refunds: a positive amount, never more in total than the item sold for."""
    checked = _checked_refunds(con)
    return checked.filter(
        (checked.amount > 0) & (checked.refunded_so_far <= checked.line_amount)
    ).drop("refunded_so_far", "line_amount")


def silver_rejected_refunds(con):
    """Refunds that can't be applied. Kept for inspection, like rejected_sales."""
    refunds = _read_bronze(con, "refunds", "refund_id").select(
        "refund_id", "item_id", "refund_date", "quantity", "amount", "reason",
    )
    return refunds.anti_join(_read(con, "silver", "refunds"), "refund_id")


# ---------------------------------------------------------------------------
# GOLD: star schema (dimensions + facts)
# ---------------------------------------------------------------------------
def gold_dim_customer(con):
    """Slowly changing dimension, type 2: one row per version of each customer."""
    history = _read(con, "silver", "customer_history")
    ordered = ibis.window(group_by="customer_id", order_by="valid_from")
    versions = history.mutate(
        version=ibis.row_number().over(ordered) + 1,
        valid_to=ibis.coalesce(history.valid_from.lead().over(ordered),
                               ibis.timestamp(FOREVER)),
    ).filter(~_.is_deleted)
    return versions.select(
        customer_key=versions.customer_id.cast("int64") * CUSTOMER_KEY_FACTOR + versions.version,
        customer_id=versions.customer_id,
        full_name=versions.full_name,
        email=versions.email,
        country=versions.country_name,
        signup_date=versions.signup_date,
        version=versions.version,
        valid_from=versions.valid_from,
        valid_to=versions.valid_to,
        is_current=versions.valid_to == ibis.timestamp(FOREVER),
    )


def _with_customer_version(con, table, date_column):
    """Add customer_key: the version of the customer valid at midnight on that date."""
    versions = _read(con, "gold", "dim_customer").select(
        "customer_key", "valid_from", "valid_to",
        version_customer_id="customer_id",
    )
    moment = table[date_column].cast("timestamp")
    return table.join(versions, [
        table.customer_id == versions.version_customer_id,
        versions.valid_from <= moment,
        moment < versions.valid_to,
    ])


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
    """A full calendar: every day of every year that has a sale or a refund."""
    bounds = []
    for table, column in (("sales", "transaction_date"), ("refunds", "refund_date")):
        dates = _read(con, "silver", table)[column]
        low, high = dates.min().execute(), dates.max().execute()
        if low is not None and low == low:  # NaT and NaN are not equal to themselves
            bounds += [low.year, high.year]
    first_year = min(bounds, default=date.today().year)
    last_year = max(bounds, default=date.today().year)

    rows = []
    day = date(first_year, 1, 1)
    while day <= date(last_year, 12, 31):
        rows.append({
            "date_key": day,
            "year": day.year,
            "quarter": (day.month - 1) // 3 + 1,
            "month": day.month,
            "month_name": calendar.month_name[day.month],
            "day": day.day,
            "day_of_week": day.isoweekday(),
            "day_name": calendar.day_name[day.weekday()],
            "is_weekend": day.isoweekday() >= 6,
        })
        day += timedelta(days=1)
    return ibis.memtable(rows, schema=ibis.schema({
        "date_key": "date", "year": "int32", "quarter": "int32", "month": "int32",
        "month_name": "string", "day": "int32", "day_of_week": "int32",
        "day_name": "string", "is_weekend": "boolean",
    }))


def gold_fact_sales(con):
    sales = _with_customer_version(con, _read(con, "silver", "sales"), "transaction_date")
    return sales.select(
        item_key=sales.item_id,
        date_key=sales.transaction_date,
        customer_key=sales.customer_key,
        product_key=sales.product_id,
        store_key=sales.store_id,
        transaction_id=sales.transaction_id,
        quantity=sales.quantity,
        unit_price=sales.unit_price,
        line_amount=sales.line_amount,
    )


def gold_fact_refunds(con):
    refunds = _with_customer_version(con, _read(con, "silver", "refunds"), "refund_date")
    return refunds.select(
        refund_key=refunds.refund_id,
        item_key=refunds.item_id,
        date_key=refunds.refund_date,
        customer_key=refunds.customer_key,
        product_key=refunds.product_id,
        store_key=refunds.store_id,
        transaction_id=refunds.transaction_id,
        quantity=refunds.quantity,
        amount=refunds.amount,
        reason=refunds.reason,
    )


# ---------------------------------------------------------------------------
# MART: business-ready data marts
# Gross figures stay as they were. Refunds are shown beside them, on the refund's date.
# ---------------------------------------------------------------------------
def _zero_if_missing(column):
    return ibis.coalesce(column, ibis.literal(0).cast(column.type()))


def mart_daily_sales(con):
    """Daily sales and refunds by store."""
    stores = _read(con, "gold", "dim_store").select("store_key", "store_name")
    sales = _read(con, "gold", "fact_sales").join(stores, "store_key")
    refunds = _read(con, "gold", "fact_refunds").join(stores, "store_key")
    sold = sales.group_by(["date_key", "store_name"]).agg(
        transaction_count=sales.transaction_id.nunique(),
        units_sold=sales.quantity.sum(),
        revenue=sales.line_amount.sum(),
    )
    refunded = refunds.group_by(["date_key", "store_name"]).agg(
        refund_amount=refunds.amount.sum(),
    ).rename(refund_date_key="date_key", refund_store_name="store_name")
    joined = sold.outer_join(refunded, [
        sold.date_key == refunded.refund_date_key,
        sold.store_name == refunded.refund_store_name,
    ])
    return joined.select(
        date_key=ibis.coalesce(joined.date_key, joined.refund_date_key),
        store_name=ibis.coalesce(joined.store_name, joined.refund_store_name),
        transaction_count=_zero_if_missing(joined.transaction_count),
        units_sold=_zero_if_missing(joined.units_sold),
        revenue=_zero_if_missing(joined.revenue),
        refund_amount=_zero_if_missing(joined.refund_amount),
    ).mutate(net_revenue=_.revenue - _.refund_amount).order_by(["date_key", "store_name"])


def mart_product_sales(con):
    """Product and category performance."""
    products = _read(con, "gold", "dim_product").select("product_key", "category", "product_name")
    sales = _read(con, "gold", "fact_sales").join(products, "product_key")
    refunds = _read(con, "gold", "fact_refunds").join(products, "product_key")
    sold = sales.group_by(["category", "product_name"]).agg(
        units_sold=sales.quantity.sum(),
        revenue=sales.line_amount.sum(),
    )
    refunded = refunds.group_by(["category", "product_name"]).agg(
        refund_amount=refunds.amount.sum(),
    )
    joined = sold.left_join(refunded, ["category", "product_name"])
    return (
        joined.mutate(refund_amount=_zero_if_missing(joined.refund_amount))
        .select("category", "product_name", "units_sold", "revenue", "refund_amount")
        .mutate(net_revenue=_.revenue - _.refund_amount)
        .order_by(ibis.desc("revenue"))
    )


def mart_customer_sales(con):
    """Customer value: one row per customer, however many versions they have."""
    dim = _read(con, "gold", "dim_customer")
    versions = dim.select("customer_key", "customer_id")
    current = dim.filter(dim.is_current).select("customer_id", "customer_key", "full_name", "country")
    sales = _read(con, "gold", "fact_sales").join(versions, "customer_key")
    refunds = _read(con, "gold", "fact_refunds").join(versions, "customer_key")
    bought = sales.group_by("customer_id").agg(
        transaction_count=sales.transaction_id.nunique(),
        total_spent=sales.line_amount.sum(),
        last_sale_date=sales.date_key.max(),
    )
    refunded = refunds.group_by("customer_id").agg(total_refunded=refunds.amount.sum())
    joined = bought.join(current, "customer_id").left_join(refunded, "customer_id")
    return (
        joined.mutate(total_refunded=_zero_if_missing(joined.total_refunded))
        .select("customer_key", "full_name", "country", "transaction_count", "total_spent",
                "last_sale_date", "total_refunded")
        .mutate(net_spent=_.total_spent - _.total_refunded)
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
    Transform("silver", "customer_history", silver_customer_history, _silver("customers")),
    Transform("silver", "products", silver_products, _bronze("products", "category_ref")),
    Transform("silver", "stores", silver_stores, _bronze("stores", "country_ref")),
    Transform("silver", "transactions", silver_transactions,
              _bronze("transactions", "status_ref", "customers")),
    Transform("silver", "sales", silver_sales,
              _bronze("transaction_items", "products")
              + _silver("transactions", "customers", "stores")),
    Transform("silver", "rejected_sales", silver_rejected_sales,
              _bronze("transaction_items") + _silver("transactions", "sales")),
    Transform("silver", "refunds", silver_refunds, _bronze("refunds") + _silver("sales")),
    Transform("silver", "rejected_refunds", silver_rejected_refunds,
              _bronze("refunds") + _silver("refunds")),
    Transform("gold", "dim_customer", gold_dim_customer, _silver("customer_history")),
    Transform("gold", "dim_product", gold_dim_product, _silver("products")),
    Transform("gold", "dim_store", gold_dim_store, _silver("stores")),
    Transform("gold", "dim_date", gold_dim_date, _silver("sales", "refunds")),
    Transform("gold", "fact_sales", gold_fact_sales, _silver("sales") + _gold("dim_customer")),
    Transform("gold", "fact_refunds", gold_fact_refunds,
              _silver("refunds") + _gold("dim_customer")),
    Transform("mart", "daily_sales_mart", mart_daily_sales,
              _gold("fact_sales", "fact_refunds", "dim_store")),
    Transform("mart", "product_sales_mart", mart_product_sales,
              _gold("fact_sales", "fact_refunds", "dim_product")),
    Transform("mart", "customer_sales_mart", mart_customer_sales,
              _gold("fact_sales", "fact_refunds", "dim_customer")),
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
