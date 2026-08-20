"""Ibis transformations: silver -> gold -> mart.

Runs on DuckDB (via Ibis), reading bronze parquet from RustFS (S3) and
writing each layer back to RustFS as parquet. This is the "one Ibis
expression, one engine" pattern from pondhouse/README.

Usage:
  python transform.py
"""
import ibis

from config import S3

LAKE = f"s3://{S3['lake_bucket']}"


def get_con():
    con = ibis.duckdb.connect()
    con.raw_sql(
        "CREATE SECRET rustfs (TYPE s3,"
        f" KEY_ID '{S3['access_key']}', SECRET '{S3['secret_key']}',"
        f" ENDPOINT '{S3['endpoint']}', URL_STYLE 'path', USE_SSL false);"
    )
    return con


def _read(con, layer, table):
    view = f"{layer}_{table}"
    con.raw_sql(
        f"CREATE OR REPLACE VIEW {view} AS"
        f" SELECT * FROM read_parquet('{LAKE}/{layer}/{table}.parquet');"
    )
    return con.table(view)


def _write(con, expr, layer, table):
    tmp = f"_out_{table}"
    con.create_table(tmp, expr, overwrite=True)
    con.raw_sql(
        f"COPY {tmp} TO '{LAKE}/{layer}/{table}.parquet'"
        " (FORMAT parquet, OVERWRITE_OR_IGNORE);"
    )
    con.raw_sql(f"DROP TABLE IF EXISTS {tmp};")


def build_silver(con):
    cust = _read(con, "bronze", "customers").select(
        "customer_id", "full_name", "email", "country", "signup_date"
    )
    prod = _read(con, "bronze", "products").select(
        "product_id", "product_name", "category", "unit_price"
    )
    orders = _read(con, "bronze", "orders").select(
        "order_id", "customer_id", "order_date", "status"
    )
    items = _read(con, "bronze", "order_items").select(
        "order_item_id", "order_id", "product_id", "quantity", "unit_price"
    )

    # customers: trim/lower/upper + dedupe on email (keep min customer_id)
    cust_clean = cust.mutate(
        full_name=cust.full_name.strip(),
        email=cust.email.lower(),
        country=cust.country.upper(),
    )
    min_cid = cust_clean.group_by("email").agg(customer_id=cust_clean.customer_id.min())
    silver_customers = min_cid.join(
        cust_clean.select("customer_id", "full_name", "country", "signup_date"),
        "customer_id",
    ).select("customer_id", "full_name", "email", "country", "signup_date")

    # products: trim name / lower category + dedupe on (name, category)
    prod_clean = prod.mutate(
        product_name=prod.product_name.strip(),
        category=prod.category.lower(),
    ).filter(prod.unit_price >= 0)
    min_pid = prod_clean.group_by(["product_name", "category"]).agg(
        product_id=prod_clean.product_id.min()
    )
    silver_products = min_pid.join(
        prod_clean.select("product_id", "unit_price"), "product_id"
    ).select("product_id", "product_name", "category", "unit_price")

    # orders: keep known statuses, normalized text
    silver_orders = orders.mutate(status=orders.status.lower()).filter(
        orders.status.lower().isin(["placed", "shipped", "delivered", "cancelled"])
    )

    # sales: order lines enriched with valid customer/product references
    silver_sales = (
        items.join(orders, "order_id")
        .join(silver_customers, "customer_id")
        .join(silver_products, "product_id")
        .mutate(line_amount=(items.quantity * items.unit_price).round(2))
        .select(
            "order_item_id",
            "order_id",
            "customer_id",
            "product_id",
            "order_date",
            "status",
            "quantity",
            "unit_price",
            "line_amount",
        )
    )

    _write(con, silver_customers, "silver", "customers")
    _write(con, silver_products, "silver", "products")
    _write(con, silver_orders, "silver", "orders")
    _write(con, silver_sales, "silver", "sales")
    print("silver written")


def build_gold(con):
    sales = _read(con, "silver", "sales")
    products = _read(con, "silver", "products")
    customers = _read(con, "silver", "customers")

    daily_sales = (
        sales.group_by("order_date")
        .agg(
            order_count=sales.order_id.nunique(),
            total_revenue=sales.line_amount.sum(),
            avg_order_value=sales.line_amount.mean().round(2),
        )
        .order_by("order_date")
    )

    category_sales = (
        sales.join(products, "product_id")
        .group_by("category")
        .agg(
            order_count=sales.order_id.nunique(),
            units_sold=sales.quantity.sum(),
            revenue=sales.line_amount.sum(),
        )
        .order_by(ibis.desc("revenue"))
    )

    customer_ltv = (
        sales.join(customers, "customer_id")
        .group_by(["customer_id", "full_name", "country"])
        .agg(
            order_count=sales.order_id.nunique(),
            total_spent=sales.line_amount.sum(),
            last_order_date=sales.order_date.max(),
        )
        .order_by(ibis.desc("total_spent"))
    )

    _write(con, daily_sales, "gold", "daily_sales")
    _write(con, category_sales, "gold", "category_sales")
    _write(con, customer_ltv, "gold", "customer_ltv")
    print("gold written")


def build_mart(con):
    sales = _read(con, "silver", "sales")
    customers = _read(con, "silver", "customers")
    products = _read(con, "silver", "products")

    dim_customer = customers.select(
        customer_key=customers.customer_id,
        customer_id=customers.customer_id,
        full_name=customers.full_name,
        email=customers.email,
        country=customers.country,
        signup_date=customers.signup_date,
    )

    dim_product = products.select(
        product_key=products.product_id,
        product_id=products.product_id,
        product_name=products.product_name,
        category=products.category,
        unit_price=products.unit_price,
    )

    dates = sales.select("order_date").distinct()
    dim_date = (
        dates.mutate(
            date_key=dates.order_date,
            year=dates.order_date.year(),
            month=dates.order_date.month(),
            day=dates.order_date.day(),
        )
        .select("date_key", "order_date", "year", "month", "day")
        .order_by("order_date")
    )

    fact_sales = sales.select(
        order_item_id=sales.order_item_id,
        date_key=sales.order_date,
        customer_key=sales.customer_id,
        product_key=sales.product_id,
        order_id=sales.order_id,
        quantity=sales.quantity,
        unit_price=sales.unit_price,
        line_amount=sales.line_amount,
    )

    _write(con, dim_customer, "mart", "dim_customer")
    _write(con, dim_product, "mart", "dim_product")
    _write(con, dim_date, "mart", "dim_date")
    _write(con, fact_sales, "mart", "fact_sales")
    print("mart written")


def run_transform():
    con = get_con()
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
