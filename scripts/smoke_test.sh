#!/usr/bin/env bash
# End-to-end smoke test: postgres -> Sling -> bronze -> silver/gold/mart -> ClickHouse,
# asserting exact numbers from the seed data in etl/sql/00_init.sql, then the same
# pipeline through Dagster, a new source row, and an outage of the lake.
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

check_marts() { # stage label, [rows], [revenue]
  local rows="${2:-6}" revenue="${3:-1656.5}"
  assert_eq "$1: fact_sales rows" "$rows" "$(clickhouse 'SELECT count() FROM marts.fact_sales')"
  assert_eq "$1: fact_sales revenue" "$revenue" \
    "$(clickhouse 'SELECT round(sum(line_amount), 2) FROM marts.fact_sales')"
  assert_eq "$1: daily mart revenue" "$revenue" \
    "$(clickhouse 'SELECT round(sum(revenue), 2) FROM marts.daily_sales_mart')"
}

echo "--- starting services"
compose up -d --wait postgres clickhouse
compose up -d rustfs
compose run --rm createbuckets

echo "--- seeding the source"
compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 \
  < etl/sql/00_init.sql >/dev/null

echo "--- pipeline"
compose --profile tools run --rm --build etl python etl.py
check_marts "first run"
assert_eq "marts are native tables" "MergeTree" \
  "$(clickhouse "SELECT engine FROM system.tables WHERE database = 'marts' AND name = 'fact_sales'")"

echo "--- pipeline again: a re-run must not multiply revenue"
compose --profile tools run --rm etl python etl.py
check_marts "re-run"
assert_eq "no temporary tables left behind" "0" \
  "$(clickhouse "SELECT count() FROM system.tables WHERE database = 'marts' AND name LIKE '%__new'")"

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
# 8 ingestion + 8 bronze + 14 silver/gold/mart + 8 clickhouse
assert_eq "dagster has one asset per table" "38" \
  "$(psql_dagster "SELECT count(*) FROM asset_keys")"
# 8 ingestion + 8 bronze + 1 gold + 8 clickhouse
assert_eq "dagster checks passed" "25" \
  "$(psql_dagster "SELECT count(*) FROM asset_check_executions WHERE execution_status = 'SUCCEEDED'")"

echo "--- a new source row must reach the marts"
compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 <<'SQL'
INSERT INTO pos.transactions (transaction_id, store_id, customer_id, transaction_date,
                              status_code, payment_method)
VALUES (9001, 1, 1, '2026-09-01', 'C', 'CARD');
INSERT INTO pos.transaction_items (transaction_id, product_id, quantity, unit_price)
VALUES (9001, 1, 1, 100.00);
SQL
compose --profile tools run --rm etl python etl.py
check_marts "new row" 7 1756.5

echo "--- lake down: the marts keep serving, and a failed rebuild must not lose them"
compose stop rustfs
check_marts "lake down" 7 1756.5
if compose --profile tools run --rm etl python serve.py >/dev/null 2>&1; then
  echo "FAIL: serve.py succeeded with the lake down" >&2
  exit 1
fi
check_marts "after failed rebuild" 7 1756.5
compose start rustfs
compose --profile tools run --rm etl python serve.py >/dev/null
check_marts "lake back" 7 1756.5

echo "smoke test passed"
