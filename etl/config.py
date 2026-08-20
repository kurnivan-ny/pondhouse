"""Connection + schedule config for the pondhouse demo ETL.

Defaults match pondhouse/.env. Override with env vars, e.g.:
  ETL_DB=demo ETL_SCHEDULE_CRON="*/10 * * * *" python scheduler.py
"""
import os


def _strip_scheme(endpoint):
    return endpoint.split("://", 1)[-1]


# --- Postgres: OLTP source (see sql/00_init.sql) ---
DB = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("ETL_DB", "demo"),
    "user": os.getenv("POSTGRES_USER", "pondhouse"),
    "password": os.getenv("POSTGRES_PASSWORD", "pondhouse"),
}

# --- RustFS (S3): lake target for migrate + ibis transforms ---
S3 = {
    "access_key": os.getenv("S3_ACCESS_KEY", "pondhouse"),
    "secret_key": os.getenv("S3_SECRET_KEY", "pondhouse-secret-change-me"),
    "endpoint": _strip_scheme(os.getenv("S3_ENDPOINT", "localhost:9000")),
    "lake_bucket": os.getenv("S3_LAKE_BUCKET", "lake"),
}

# 5-field cron: minute hour day month weekday
SCHEDULE_CRON = os.getenv("ETL_SCHEDULE_CRON", "*/5 * * * *")
