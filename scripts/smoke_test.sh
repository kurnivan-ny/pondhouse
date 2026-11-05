#!/usr/bin/env bash
# End-to-end smoke test: postgres -> ingestion -> bronze -> silver/gold/mart ->
# ClickHouse, asserting exact numbers from the seed data in etl/sql/00_init.sql.
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
compose up -d --wait postgres
compose up -d rustfs clickhouse
compose run --rm createbuckets
for _ in $(seq 1 60); do
  clickhouse 'SELECT 1' >/dev/null 2>&1 && break
  sleep 2
done
clickhouse 'SELECT 1' >/dev/null

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

echo "smoke test passed"
