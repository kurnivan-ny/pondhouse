"""Thin psycopg2 helpers: connect, run SQL, run a .sql file."""
from contextlib import closing

import psycopg2

from config import DB


def get_conn():
    return psycopg2.connect(**DB)


# NOTE: `with connection` only wraps a transaction in psycopg2 — it commits or
# rolls back but leaves the socket open. Every helper below therefore wraps the
# connection in closing() as well, otherwise long-running callers (scheduler.py
# runs these on a cron) leak one connection per call until Postgres refuses more.


def execute(sql, params=None):
    with closing(get_conn()) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)


def execute_file(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with closing(get_conn()) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql)


def query(sql, params=None):
    with closing(get_conn()) as conn:
        with conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
