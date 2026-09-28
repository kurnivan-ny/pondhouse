"""Insert new POS transactions into the source (simulates manual entry).

Usage:
  python generate_data.py            # insert one random transaction
  python generate_data.py --rows 5   # insert five random transactions
"""
import argparse
import random
from datetime import date, timedelta

import db

STATUSES = ["C", "P", "R", "X"]
PAYMENTS = ["CASH", "CARD", "EWALLET"]


def load_lookup(conn):
    cur = conn.cursor()
    cur.execute("SELECT customer_id FROM pos.customers")
    customer_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT store_id FROM pos.stores")
    store_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT product_id, unit_price FROM pos.products")
    products = cur.fetchall()
    return customer_ids, store_ids, products


def insert_transaction(conn, customer_ids, store_ids, products):
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO pos.transactions (store_id, customer_id, transaction_date, status_code, payment_method)"
        " VALUES (%s, %s, %s, %s, %s) RETURNING transaction_id",
        (
            random.choice(store_ids),
            random.choice(customer_ids),
            date.today() - timedelta(days=random.randint(0, 30)),
            random.choice(STATUSES),
            random.choice(PAYMENTS),
        ),
    )
    transaction_id = cur.fetchone()[0]
    for _ in range(random.randint(1, 3)):
        product_id, unit_price = random.choice(products)
        cur.execute(
            "INSERT INTO pos.transaction_items (transaction_id, product_id, quantity, unit_price)"
            " VALUES (%s, %s, %s, %s)",
            (transaction_id, product_id, random.randint(1, 4), unit_price),
        )
    return transaction_id


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1, help="number of transactions to insert")
    args = parser.parse_args()

    conn = db.get_conn()
    try:
        customer_ids, store_ids, products = load_lookup(conn)
        for _ in range(args.rows):
            transaction_id = insert_transaction(conn, customer_ids, store_ids, products)
            print(f"inserted transaction {transaction_id}")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
