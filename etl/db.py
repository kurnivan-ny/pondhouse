"""Thin psycopg2 helpers: connect, run SQL, run a .sql file."""
import psycopg2

from config import DB


def get_conn():
    return psycopg2.connect(**DB)


def execute(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)


def execute_file(path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)


def query(sql, params=None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
