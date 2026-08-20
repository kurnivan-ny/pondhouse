"""Insert new sample orders into the oltp source (simulates manual entry).

Usage:
  python generate_data.py            # insert one random order
  python generate_data.py --rows 5   # insert five random orders
"""
import argparse
import random
from datetime import date, timedelta

import db

STATUSES = ["placed", "shipped", "delivered", "cancelled"]


def load_lookup(conn):
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM oltp.customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, unit_price FROM oltp.products")
    products = cur.fetchall()
    return customer_ids, products


def insert_order(conn, customer_ids, products):
    cur = conn.cursor()
    customer_id = random.choice(customer_ids)
    order_date = date.today() - timedelta(days=random.randint(0, 30))
    status = random.choice(STATUSES)
    cur.execute(
        "INSERT INTO oltp.orders (customer_id, order_date, status)"
        " VALUES (%s, %s, %s) RETURNING order_id",
        (customer_id, order_date, status),
    )
    order_id = cur.fetchone()[0]
    for _ in range(random.randint(1, 3)):
        product_id, unit_price = random.choice(products)
        cur.execute(
            "INSERT INTO oltp.order_items (order_id, product_id, quantity, unit_price)"
            " VALUES (%s, %s, %s, %s)",
            (order_id, product_id, random.randint(1, 4), unit_price),
        )
    return order_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1, help="number of orders to insert")
    args = parser.parse_args()

    conn = db.get_conn()
    try:
        customer_ids, products = load_lookup(conn)
        for _ in range(args.rows):
            order_id = insert_order(conn, customer_ids, products)
            print(f"inserted order {order_id}")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
