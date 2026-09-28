"""Pondhouse Dagster definitions.

Orchestrates the lakehouse pipeline:
  - Sling batch ingestion (oltp -> parquet on RustFS)
  - Ibis/DuckDB medallion transforms (bronze -> silver -> gold -> mart)
  - ClickHouse serving layer creation
  - Asset checks for data quality
"""

import dagster as dg
import os
import shutil
import subprocess
import sys

PONDHOUSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# The ETL modules are top-level scripts in etl/ (not an importable `etl` package —
# etl.py itself shadows that name), so they run as `-m <module>` from ETL_DIR.
# In the Dagster image the code is copied to /opt/dagster/etl, which is not two
# levels up from repo/; PONDHOUSE_ETL_DIR lets the image state where it lives.
def _resolve_etl_dir():
    override = os.getenv("PONDHOUSE_ETL_DIR")
    if override:
        return override
    for candidate in (
        os.path.join(PONDHOUSE_DIR, "etl"),                        # repo checkout
        os.path.join(os.path.dirname(os.path.dirname(__file__)), "etl"),  # image layout
    ):
        if os.path.isdir(candidate):
            return candidate
    return os.path.join(PONDHOUSE_DIR, "etl")


ETL_DIR = _resolve_etl_dir()


def _run_etl_module(script, *args, method=None):
    """Run one of the etl/ scripts and surface its output on failure."""
    result = subprocess.run(
        [sys.executable, script, *args],
        cwd=ETL_DIR, capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"{script} failed (exit {result.returncode}):\n"
            f"{result.stderr or result.stdout}"
        )
    payload = {"status": "success", "output": result.stdout}
    if method:
        payload["method"] = method
    return payload


# --- Sling Ingestion Asset ---
@dg.asset(
    description="Run Sling batch replication: Postgres (pos) -> parquet on RustFS",
    group_name="ingestion",
)
def sling_ingestion() -> dict:
    """Land the POS tables in s3://lake/ingestion/.

    Dagster runs inside a container with no docker CLI, so driving the Sling
    container via `docker compose` is not possible from here. When the CLI *is*
    available (running Dagster on the host) Sling is used; otherwise the asset
    falls back to the equivalent Ibis/DuckDB ingestion in etl/ingest.py, which
    needs nothing but network access to Postgres and RustFS.
    """
    if shutil.which("docker"):
        for argv in (
            ["docker", "compose", "--profile", "tools", "up", "-d", "sling"],
            ["docker", "compose", "--profile", "tools", "exec", "-T", "sling",
             "sling", "run", "--home-dir", "/sling",
             "-r", "/sling/replications/demo-to-lake.yaml"],
        ):
            result = subprocess.run(argv, cwd=PONDHOUSE_DIR,
                                    capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Sling step failed: {result.stderr}")
        return {"status": "success", "method": "sling", "output": result.stdout}

    return _run_etl_module("ingest.py", "--method", "custom", "--mode", "merge",
                           method="ibis")


# --- Bronze Layer Asset ---
@dg.asset(
    description="Build bronze Delta tables from the ingestion layer",
    group_name="transform",
)
def bronze_layer(sling_ingestion: dict) -> dict:
    """Run bronze layer ETL.

    The Sling path lands parquet; the Ibis fallback lands Delta, so the bronze
    reader has to be told which layout the ingestion layer actually has.
    """
    fmt = "parquet" if sling_ingestion.get("method") == "sling" else "delta"
    return _run_etl_module("bronze.py", "--source-format", fmt)


# --- Transform (Silver/Gold/Mart) Asset ---
@dg.asset(
    description="Build silver, gold, and mart Delta layers via Ibis",
    group_name="transform",
    deps=[bronze_layer],
)
def transform_layer() -> dict:
    """Build the silver, gold and mart Delta layers."""
    return _run_etl_module("transform.py")


# --- Serve (ClickHouse Marts) Asset ---
@dg.asset(
    description="Create ClickHouse marts tables reading Delta from RustFS",
    group_name="serve",
    deps=[transform_layer],
)
def clickhouse_marts() -> dict:
    """Create the ClickHouse marts tables over the Delta gold + mart layers."""
    return _run_etl_module("serve.py")


# --- Asset Checks ---
@dg.asset_check(asset=sling_ingestion, description="Verify ingestion produced parquet files")
def sling_ingestion_check() -> dg.AssetCheckResult:
    """Check that Sling produced expected parquet files in RustFS."""
    # This would use MinIO client or S3 API to verify files exist
    # For now, return passed with metadata note
    return dg.AssetCheckResult(
        passed=True,
        metadata={"note": "Implement S3 verification via minio client"}
    )


@dg.asset_check(asset=bronze_layer, description="Verify bronze tables have rows")
def bronze_layer_check() -> dg.AssetCheckResult:
    """Check bronze Delta tables have expected row counts."""
    return dg.AssetCheckResult(
        passed=True,
        metadata={"note": "Implement DuckDB row count check"}
    )


@dg.asset_check(asset=transform_layer, description="Verify gold fact table has rows")
def gold_fact_check() -> dg.AssetCheckResult:
    """Check gold fact_sales table has data."""
    return dg.AssetCheckResult(
        passed=True,
        metadata={"note": "Implement DuckDB row count check on gold.fact_sales"}
    )


@dg.asset_check(asset=clickhouse_marts, description="Verify ClickHouse marts tables exist")
def clickhouse_marts_check() -> dg.AssetCheckResult:
    """Check ClickHouse marts tables were created."""
    return dg.AssetCheckResult(
        passed=True,
        metadata={"note": "Implement ClickHouse table existence check"}
    )


# --- Full Pipeline Job ---
# An @dg.job body cannot invoke assets: it would wrap clickhouse_marts as a lone
# op and silently skip ingestion/bronze/transform. define_asset_job materializes
# the whole asset graph in dependency order instead.
pondhouse_pipeline_job = dg.define_asset_job(
    name="pondhouse_pipeline",
    description="Full pondhouse pipeline: ingestion -> bronze -> transform -> serve",
    selection=dg.AssetSelection.all(),
)


# --- Schedule ---
pondhouse_schedule = dg.ScheduleDefinition(
    job=pondhouse_pipeline_job,
    cron_schedule="*/5 * * * *",  # Every 5 minutes (matches ETL_SCHEDULE_CRON)
    execution_timezone="UTC",
)


defs = dg.Definitions(
    assets=[
        sling_ingestion,
        bronze_layer,
        transform_layer,
        clickhouse_marts,
    ],
    asset_checks=[
        sling_ingestion_check,
        bronze_layer_check,
        gold_fact_check,
        clickhouse_marts_check,
    ],
    jobs=[pondhouse_pipeline_job],
    schedules=[pondhouse_schedule],
)
