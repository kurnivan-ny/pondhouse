"""Ibis transformations: bronze -> silver -> gold -> mart (all Delta on RustFS).

Retail medallion:
  silver — translate (join reference lookups), cleanse, deduplicate
  gold   — star schema (dim_* + fact_sales)
  mart   — business-ready data marts

Usage:
  python transform.py
"""
import ibis
from ibis import _

from config import S3
from conn import get_conn, read_delta, write_delta


UNKNOWN = "Unknown"


def _read(con, layer, table):
    path = f"s3://{S3['lake_bucket']}/{layer}/{table}"
    return read_delta(con, path, f"{layer}_{table}")


def _dedupe(table, key):
    """Keep one row per primary key, so re-ingested rows cannot fan out joins.

    Append-mode ingestion stamps `_loaded_at`, so the newest version wins there.
    Other modes cannot produce duplicates with differing values.
    """
    order = [ibis.desc("_loaded_at")] if "_loaded_at" in table.columns else []
    window = ibis.window(group_by=key, order_by=[*order, key])
    return table.mutate(_rn=ibis.row_number().over(window)).filter(_._rn == 0).drop("_rn")


def _read_bronze(con, table, key):
    return _dedupe(_read(con, "bronze", table), key)


# ---------------------------------------------------------------------------
# SILVER: translate + cleanse + deduplicate
# ---------------------------------------------------------------------------
def build_silver(con):
    customers = _read_bronze(con, "customers", "customer_id")
    products = _read_bronze(con, "products", "product_id")
    stores = _read_bronze(con, "stores", "store_id")
    transactions = _read_bronze(con, "transactions", "transaction_id")
    items = _read_bronze(con, "transaction_items", "item_id")
    category_ref = _read_bronze(con, "category_ref", "category_code")
    status_ref = _read_bronze(con, "status_ref", "status_code")
    country_ref = _read_bronze(con, "country_ref", "country_code")

    # customers: trim/lower/upper + dedupe on email + translate country code.
    # An unknown code keeps the customer, labelled "Unknown", so their sales stay.
    cust_clean = customers.mutate(
        full_name=customers.full_name.strip(),
        email=customers.email.lower(),
        country_code=customers.country_code.upper(),
    )
    min_cid = cust_clean.group_by("email").agg(customer_id=cust_clean.customer_id.min())
    # Every source id -> the id that survived dedup, so sales follow the survivor.
    customer_map = cust_clean.select("customer_id", "email").join(
        min_cid.rename(surviving_customer_id="customer_id"), "email"
    ).select("customer_id", "surviving_customer_id")
    silver_customers = (
        min_cid.join(
            cust_clean.select("customer_id", "full_name", "country_code", "signup_date"),
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

    # products: trim name + valid price + dedupe on (name, category) + translate category code
    prod_clean = products.mutate(
        product_name=products.product_name.strip(),
        category_code=products.category_code.upper(),
    ).filter(products.unit_price >= 0)
    min_pid = prod_clean.group_by(["product_name", "category_code"]).agg(
        product_id=prod_clean.product_id.min()
    )
    product_map = prod_clean.select("product_id", "product_name", "category_code").join(
        min_pid.rename(surviving_product_id="product_id"),
        ["product_name", "category_code"],
    ).select("product_id", "surviving_product_id")
    silver_products = (
        min_pid.join(
            prod_clean.select("product_id", "unit_price"), "product_id"
        )
        .left_join(category_ref, "category_code")
        .mutate(category_name=ibis.coalesce(_.category_name, UNKNOWN))
        .select("product_id", "product_name", "category_code", "category_name", "unit_price")
    )

    # stores: clean + translate country code
    silver_stores = (
        stores.mutate(
            store_name=stores.store_name.strip(),
            city=stores.city.strip(),
            country_code=stores.country_code.upper(),
        )
        .left_join(country_ref, "country_code")
        .mutate(country_name=ibis.coalesce(_.country_name, UNKNOWN))
        .select("store_id", "store_name", "city", "country_code", "country_name")
    )

    # transactions: keep completed + translate status code + follow the surviving customer
    silver_transactions = (
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

    # sales fact-atom: items of completed transactions, following the surviving
    # product, joined to valid dims, line_amount computed
    items_s = items.select("item_id", "transaction_id", "product_id", "quantity", "unit_price")
    completed_items = items_s.join(silver_transactions, "transaction_id")
    silver_sales = (
        completed_items.join(product_map, "product_id")
        .mutate(
            product_id=_.surviving_product_id,
            line_amount=(_.quantity * _.unit_price).round(2),
        )
        .semi_join(silver_customers, "customer_id")
        .semi_join(silver_stores, "store_id")
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

    # Completed items that could not become a sale (e.g. an invalid product).
    # Kept for inspection, so sales + rejected always reconciles with the source.
    rejected_sales = completed_items.anti_join(silver_sales, "item_id").select(
        "item_id", "transaction_id", "store_id", "customer_id", "product_id",
        "quantity", "unit_price",
    )

    write_delta(con, silver_customers, "silver", "customers")
    write_delta(con, silver_products, "silver", "products")
    write_delta(con, silver_stores, "silver", "stores")
    write_delta(con, silver_transactions, "silver", "transactions")
    write_delta(con, silver_sales, "silver", "sales")
    write_delta(con, rejected_sales, "silver", "rejected_sales")


# ---------------------------------------------------------------------------
# GOLD: star schema (dimensions + fact)
# ---------------------------------------------------------------------------
def build_gold(con):
    customers = _read(con, "silver", "customers")
    products = _read(con, "silver", "products")
    stores = _read(con, "silver", "stores")
    sales = _read(con, "silver", "sales")

    dim_customer = customers.select(
        customer_key=customers.customer_id,
        customer_id=customers.customer_id,
        full_name=customers.full_name,
        email=customers.email,
        country=customers.country_name,
        signup_date=customers.signup_date,
    )

    dim_product = products.select(
        product_key=products.product_id,
        product_id=products.product_id,
        product_name=products.product_name,
        category=products.category_name,
        unit_price=products.unit_price,
    )

    dim_store = stores.select(
        store_key=stores.store_id,
        store_id=stores.store_id,
        store_name=stores.store_name,
        city=stores.city,
        country=stores.country_name,
    )

    dates = sales.select("transaction_date").distinct()
    dim_date = (
        dates.mutate(
            date_key=dates.transaction_date,
            year=dates.transaction_date.year(),
            month=dates.transaction_date.month(),
            day=dates.transaction_date.day(),
        )
        .select("date_key", "transaction_date", "year", "month", "day")
        .order_by("transaction_date")
    )

    fact_sales = sales.select(
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

    write_delta(con, dim_customer, "gold", "dim_customer")
    write_delta(con, dim_product, "gold", "dim_product")
    write_delta(con, dim_store, "gold", "dim_store")
    write_delta(con, dim_date, "gold", "dim_date")
    write_delta(con, fact_sales, "gold", "fact_sales")


# ---------------------------------------------------------------------------
# MART: business-ready data marts
# ---------------------------------------------------------------------------
def build_mart(con):
    sales = _read(con, "gold", "fact_sales")
    dim_product = _read(con, "gold", "dim_product")
    dim_store = _read(con, "gold", "dim_store")
    dim_customer = _read(con, "gold", "dim_customer")

    # daily sales by store
    daily_sales_mart = (
        sales.join(dim_store, "store_key")
        .group_by(["date_key", "store_name"])
        .agg(
            transaction_count=sales.transaction_id.nunique(),
            units_sold=sales.quantity.sum(),
            revenue=sales.line_amount.sum(),
        )
        .order_by(["date_key", "store_name"])
    )

    # product / category performance
    product_sales_mart = (
        sales.join(dim_product, "product_key")
        .group_by(["category", "product_name"])
        .agg(
            units_sold=sales.quantity.sum(),
            revenue=sales.line_amount.sum(),
        )
        .order_by(ibis.desc("revenue"))
    )

    # customer value
    customer_sales_mart = (
        sales.join(dim_customer, "customer_key")
        .group_by(["customer_key", "full_name", "country"])
        .agg(
            transaction_count=sales.transaction_id.nunique(),
            total_spent=sales.line_amount.sum(),
            last_sale_date=sales.date_key.max(),
        )
        .order_by(ibis.desc("total_spent"))
    )

    write_delta(con, daily_sales_mart, "mart", "daily_sales_mart")
    write_delta(con, product_sales_mart, "mart", "product_sales_mart")
    write_delta(con, customer_sales_mart, "mart", "customer_sales_mart")


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
