"""Batch migration: Postgres oltp -> bronze parquet on RustFS via Sling.

Runs the Sling CLI (pondhouse `tools` profile) using the replication in
../../ingestion/sling/replications/demo-to-lake.yaml.

Usage:
  python migrate.py
"""
import os
import subprocess

PONDHOUSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPLICATION = "/sling/replications/demo-to-lake.yaml"


def run_migrate():
    # Ensure the sling container exists/is up (pulls the image on first run).
    subprocess.run(
        ["docker", "compose", "--profile", "tools", "up", "-d", "sling"],
        check=True, cwd=PONDHOUSE_DIR,
    )
    subprocess.run(
        ["docker", "compose", "--profile", "tools", "exec", "-T", "sling",
         "sling", "run", "--home-dir", "/sling", "-r", REPLICATION],
        check=True, cwd=PONDHOUSE_DIR,
    )
    print("migrated oltp.* -> s3://lake/bronze/*.parquet (Sling)")


if __name__ == "__main__":
    run_migrate()
