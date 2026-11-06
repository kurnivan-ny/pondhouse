"""Pondhouse Dagster definitions: one asset per table.

  ingestion/*  Sling replication, Postgres -> parquet on RustFS (runs as one step)
  bronze/*     Delta copy of each source table
  silver/*, gold/*, mart/*  Ibis transforms, from the registry in etl/transform.py
  clickhouse/* native ClickHouse tables, swapped in atomically

Assets call the etl/ Python functions directly.
"""

import os
import sys

import dagster as dg

PONDHOUSE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))

# The ETL modules are top-level scripts in etl/, not an importable package. In the
# Dagster image they live at /opt/dagster/etl, so PONDHOUSE_ETL_DIR says where.
ETL_DIR = os.getenv("PONDHOUSE_ETL_DIR", os.path.join(PONDHOUSE_DIR, "etl"))
sys.path.insert(0, ETL_DIR)

import alerts  # noqa: E402
import bronze  # noqa: E402
import checks  # noqa: E402
import ingest  # noqa: E402
import serve  # noqa: E402
import transform  # noqa: E402
from config import OPTIONAL_TABLES, TABLES  # noqa: E402
from conn import get_conn  # noqa: E402


def _key(layer, table):
    return dg.AssetKey([layer, table])


def _build_with_con(build):
    con = get_conn()
    try:
        build(con)
    finally:
        con.disconnect()


# --- Assets ---
@dg.multi_asset(
    name="sling_replication",
    specs=[dg.AssetSpec(_key("ingestion", t), group_name="ingestion") for t in TABLES],
    description="Sling replication: Postgres (pos) -> parquet on RustFS",
)
def sling_replication():
    ingest.run_sling()
    for table in TABLES:
        yield dg.MaterializeResult(asset_key=_key("ingestion", table))


def _bronze_asset(table):
    @dg.asset(key=_key("bronze", table), deps=[_key("ingestion", table)], group_name="bronze")
    def asset() -> None:
        _build_with_con(lambda con: bronze.build_table(con, table))
    return asset


def _transform_asset(spec):
    @dg.asset(
        key=_key(spec.layer, spec.table),
        deps=[_key(*dep) for dep in spec.deps],
        group_name=spec.layer,
    )
    def asset() -> None:
        _build_with_con(lambda con: transform.build_table(con, spec))
    return asset


def _clickhouse_asset(table, layer):
    @dg.asset(key=_key("clickhouse", table), deps=[_key(layer, table)], group_name="clickhouse")
    def asset() -> None:
        serve.serve_table(table)
    return asset


# --- Asset checks ---
def _check_result(run):
    """A check that cannot run counts as failed, rather than failing the whole run."""
    try:
        outcome = run()
    except Exception as e:
        return dg.AssetCheckResult(passed=False, metadata={"error": str(e)[-2000:]})
    return dg.AssetCheckResult(
        passed=outcome["passed"],
        metadata={k: dg.MetadataValue.json(v) if isinstance(v, (dict, list)) else v
                  for k, v in outcome["metadata"].items()},
    )


def _rows_check(layer, table):
    @dg.asset_check(asset=_key(layer, table), name="has_rows",
                    description="The table has rows")
    def check() -> dg.AssetCheckResult:
        return _check_result(lambda: checks.run_table_rows(layer, table))
    return check


def _served_check(table):
    @dg.asset_check(asset=_key("clickhouse", table), name="matches_delta",
                    description="ClickHouse serves the same row count as the Delta table")
    def check() -> dg.AssetCheckResult:
        return _check_result(lambda: checks.run_served_table(table))
    return check


@dg.asset_check(
    asset=_key("gold", "fact_sales"),
    description="fact_sales is unique, resolves against dims, and reconciles with silver",
)
def gold_fact_check() -> dg.AssetCheckResult:
    return _check_result(lambda: checks.run("gold"))


# --- Failure alerts ---
# These run in dagster-daemon and are on by default. Each run sends at most one message,
# since one outage fails many checks. Nothing is sent until ALERT_WEBHOOK_URL is set.
def _failed_checks(instance, run_id):
    events = instance.all_logs(run_id, of_type=dg.DagsterEventType.ASSET_CHECK_EVALUATION)
    evaluations = [event.dagster_event.event_specific_data for event in events]
    return sorted(
        f"{'/'.join(e.asset_key.path)} {e.check_name}" for e in evaluations if not e.passed
    )


def _failed_checks_text(failed):
    shown = ", ".join(failed[:10])
    more = f" and {len(failed) - 10} more" if len(failed) > 10 else ""
    return f"{len(failed)} failed checks: {shown}{more}"


@dg.run_failure_sensor(default_status=dg.DefaultSensorStatus.RUNNING,
                       monitor_all_code_locations=True)
def alert_on_run_failure(context: dg.RunFailureSensorContext):
    run = context.dagster_run
    failed = _failed_checks(context.instance, run.run_id)
    checks_text = f" {_failed_checks_text(failed)}." if failed else ""
    alerts.send_alert(
        f"pondhouse: run {run.run_id} of {run.job_name} failed: "
        f"{context.failure_event.message}{checks_text}"
    )


# A failed check does not fail the run, so the sensor above would miss it.
@dg.run_status_sensor(run_status=dg.DagsterRunStatus.SUCCESS,
                      default_status=dg.DefaultSensorStatus.RUNNING,
                      monitor_all_code_locations=True)
def alert_on_failed_checks(context: dg.RunStatusSensorContext):
    run = context.dagster_run
    failed = _failed_checks(context.instance, run.run_id)
    if failed:
        alerts.send_alert(
            f"pondhouse: run {run.run_id} of {run.job_name} succeeded "
            f"with {_failed_checks_text(failed)}"
        )


# --- Job and schedule ---
pondhouse_pipeline_job = dg.define_asset_job(
    name="pondhouse_pipeline",
    description="Full pondhouse pipeline: ingestion -> bronze -> transform -> serve",
    selection=dg.AssetSelection.all(),
)

pondhouse_schedule = dg.ScheduleDefinition(
    job=pondhouse_pipeline_job,
    cron_schedule=os.getenv("ETL_SCHEDULE_CRON", "*/5 * * * *"),
    execution_timezone="UTC",
)

defs = dg.Definitions(
    assets=[
        sling_replication,
        *[_bronze_asset(t) for t in TABLES],
        *[_transform_asset(spec) for spec in transform.TRANSFORMS],
        *[_clickhouse_asset(t, layer) for t, (layer, _sort_key) in serve.MARTS.items()],
    ],
    asset_checks=[
        *[_rows_check("ingestion", t) for t in TABLES if t not in OPTIONAL_TABLES],
        *[_rows_check("bronze", t) for t in TABLES if t not in OPTIONAL_TABLES],
        gold_fact_check,
        *[_served_check(t) for t in serve.MARTS],
    ],
    jobs=[pondhouse_pipeline_job],
    schedules=[pondhouse_schedule],
    sensors=[alert_on_run_failure, alert_on_failed_checks],
    # One process for the whole run. A subprocess per step would start Python and
    # import Ibis about 60 times, which costs far more time and memory than this
    # small pipeline saves by running in parallel.
    executor=dg.in_process_executor,
)
