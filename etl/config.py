"""Connection + schedule config for the pondhouse demo ETL.

Defaults match pondhouse/.env. Override with env vars, e.g.:
  ETL_DB=demo ETL_SCHEDULE_CRON="*/10 * * * *" python scheduler.py
"""
import os
from urllib.parse import urlparse


def _parse_endpoint(endpoint):
    """Parse S3 endpoint, handling both with and without scheme."""
    if "://" not in endpoint:
        # No scheme, assume http
        return "http", endpoint
    parsed = urlparse(endpoint)
    return parsed.scheme, parsed.netloc


# Parse endpoint once
_S3_SCHEME, _S3_ENDPOINT = _parse_endpoint(os.getenv("S3_ENDPOINT", "http://localhost:9000"))

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
    "endpoint": _S3_ENDPOINT,
    "scheme": _S3_SCHEME,
    "lake_bucket": os.getenv("S3_LAKE_BUCKET", "lake"),
}


def s3_storage_options():
    """Storage options for deltalake reads/writes against RustFS."""
    # Use the parsed scheme (http or https)
    endpoint_url = f"{S3['scheme']}://{S3['endpoint']}"
    return {
        "AWS_ACCESS_KEY_ID": S3["access_key"],
        "AWS_SECRET_ACCESS_KEY": S3["secret_key"],
        "AWS_ENDPOINT_URL": endpoint_url,
        "AWS_REGION": os.getenv("S3_REGION", "us-east-1"),
        "AWS_ALLOW_HTTP": "true" if S3["scheme"] == "http" else "false",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


# 5-field cron: minute hour day month weekday
SCHEDULE_CRON = os.getenv("ETL_SCHEDULE_CRON", "*/5 * * * *")
