#!/usr/bin/env bash
# End-to-end smoke test: postgres -> ingestion -> bronze -> silver/gold/mart ->
# ClickHouse, asserting exact numbers from the seed data in etl/sql/00_init.sql,
# then the same pipeline through Dagster.
#
# Runs in its own compose project with its own volumes and no published ports, so
# it never touches a real `pondhouse` stack. Everything it creates is removed on exit.
set -euo pipefail

cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

PROJECT=pondhouse-smoke
compose() {
  docker compose -p "$PROJECT" -f docker-compose.yml -f docker-compose.smoke.yml "$@"
}
trap 'compose --profile tools down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

env_value() { sed -n "s/^$1=//p" .env | head -1; }
POSTGRES_USER="$(env_value POSTGRES_USER)"
CLICKHOUSE_PASSWORD="$(env_value CLICKHOUSE_PASSWORD)"

clickhouse() {
  compose exec -T clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "$1"
}
psql_dagster() {
  compose exec -T postgres psql -qtA -U "$POSTGRES_USER" -d dagster -c "$1"
}

assert_eq() { # description, expected, actual
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1: expected '$2', got '$3'" >&2
    exit 1
  fi
  echo "ok: $1 = $3"
}

check_marts() { # stage label
  assert_eq "$1: fact_sales rows" "6" "$(clickhouse 'SELECT count() FROM marts.fact_sales')"
  assert_eq "$1: fact_sales revenue" "1656.5" \
    "$(clickhouse 'SELECT round(sum(line_amount), 2) FROM marts.fact_sales')"
  assert_eq "$1: daily mart revenue" "1656.5" \
    "$(clickhouse 'SELECT round(sum(revenue), 2) FROM marts.daily_sales_mart')"
}

echo "--- starting services"
compose up -d --wait postgres clickhouse
compose up -d rustfs
compose run --rm createbuckets

echo "--- seeding the source"
compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 \
  < etl/sql/00_init.sql >/dev/null

echo "--- pipeline (overwrite)"
compose --profile tools run --rm --build etl python etl.py --ingest custom
check_marts "overwrite"

echo "--- pipeline (append, twice): re-reads must not multiply revenue"
compose --profile tools run --rm etl python etl.py --ingest custom --mode append
compose --profile tools run --rm etl python etl.py --ingest custom --mode append
check_marts "append x2"

echo "--- pipeline (merge): must converge to the same result"
compose --profile tools run --rm etl python etl.py --ingest custom --mode merge
check_marts "merge"

echo "--- pipeline (Sling ingestion -> parquet landing)"
export SLING_USER="$(id -u):$(id -g)"
compose --profile tools up -d sling
compose --profile tools exec -T sling \
  sling run --home-dir /sling -r /sling/replications/demo-to-lake.yaml
compose --profile tools run --rm etl python bronze.py --source-format parquet
compose --profile tools run --rm etl python transform.py
compose --profile tools run --rm etl python serve.py
check_marts "sling"

echo "--- data-quality checks"
for layer in ingestion bronze gold marts; do
  passed="$(compose --profile tools run --rm etl python checks.py "$layer" 2>/dev/null \
    | tail -1 | python3 -c 'import json,sys; print(json.load(sys.stdin)["passed"])')"
  assert_eq "check $layer" "True" "$passed"
done

echo "--- dagster: materialize the asset graph, with state in postgres"
compose up -d --build --wait dagster-webserver
compose exec -T -w /opt/dagster -e PYTHONPATH=/opt/dagster dagster-webserver \
  dagster asset materialize --select '*' -m repo.definitions >/dev/null
check_marts "dagster"
assert_eq "dagster run stored in postgres" "1" \
  "$(psql_dagster "SELECT count(*) FROM runs WHERE status = 'SUCCESS'")"
assert_eq "dagster checks passed" "4" \
  "$(psql_dagster "SELECT count(*) FROM asset_check_executions WHERE execution_status = 'SUCCEEDED'")"

echo "smoke test passed"
