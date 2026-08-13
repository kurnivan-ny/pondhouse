"""Pondhouse Dagster definitions.

Replace the examples below with real jobs, e.g.:
  - Sling batch replications  (../../ingestion/sling/)
  - SQLMesh runs              (../../transformations/)
  - Ibis-based asset checks against DuckDB / ClickHouse
"""

import dagster as dg


@dg.asset(description="Placeholder lake asset — swap for a sling/sqlmesh job.")
def bronze_example() -> str:
    return "pondhouse"


@dg.asset_check(asset=bronze_example, description="Example quality gate.")
def bronze_example_has_rows() -> dg.AssetCheckResult:
    # Real example: row-count / null-rate check via ibis against DuckDB or ClickHouse.
    return dg.AssetCheckResult(passed=True, metadata={"note": "replace me"})


defs = dg.Definitions(
    assets=[bronze_example],
    asset_checks=[bronze_example_has_rows],
)
