"""Ingestion layer: Sling replication, postgres `pos.*` -> s3://lake/ingestion/{table}.parquet

Sling is installed in the etl and Dagster images (see pyproject.toml). Its connections
and replication live in SLING_HOME, which the images point at their copy of
ingestion/sling.

Usage:
  python ingest.py
"""
import os
import subprocess

SLING_HOME = os.getenv(
    "SLING_HOME",
    os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "ingestion", "sling")),
)
REPLICATION = os.path.join(SLING_HOME, "replications", "demo-to-lake.yaml")


def run_sling():
    """Run the full-refresh replication, raising with Sling's output if it fails."""
    result = subprocess.run(
        ["sling", "run", "--home-dir", SLING_HOME, "-r", REPLICATION],
        capture_output=True, text=True, check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Sling failed (exit {result.returncode}):\n{result.stderr or result.stdout}"
        )
    print("ingested pos.* -> s3://lake/ingestion/*.parquet (Sling)")


if __name__ == "__main__":
    run_sling()
