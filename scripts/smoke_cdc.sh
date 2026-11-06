#!/usr/bin/env bash
# End-to-end CDC check: a row inserted in postgres must reach s3://cdc-landing as
# parquet, through Debezium -> Kafka -> the Aiven S3 sink.
#
# Same isolation as smoke_test.sh: its own compose project, volumes and no published
# ports. Everything it creates is removed on exit.
set -euo pipefail

cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

PROJECT=pondhouse-smoke-cdc
compose() {
  docker compose -p "$PROJECT" -f docker-compose.yml -f docker-compose.smoke.yml --profile cdc "$@"
}
trap 'compose down -v --remove-orphans >/dev/null 2>&1 || true' EXIT

env_value() { sed -n "s/^$1=//p" .env | head -1; }
POSTGRES_USER="$(env_value POSTGRES_USER)"

connect_api() { # method, path, [json file]
  local args=(-sS -X "$1" "http://localhost:8083$2" -H "Content-Type: application/json")
  if [ $# -ge 3 ]; then
    compose exec -T connect curl "${args[@]}" -d @- < "$3"
  else
    compose exec -T connect curl "${args[@]}"
  fi
}

landing_files() {
  compose run --rm -T --entrypoint /bin/sh createbuckets -c \
    'rc alias set rustfs http://rustfs:9000 "$S3_ACCESS_KEY" "$S3_SECRET_KEY" -q && rc object list --recursive rustfs/cdc-landing' 2>/dev/null
}

echo "--- starting services"
compose up -d --wait postgres
compose up -d rustfs
compose run --rm createbuckets
compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 \
  < etl/sql/00_init.sql >/dev/null
compose up -d --build --wait connect

echo "--- waiting for the Connect REST API"
# The web server answers 404 until Connect has started, so wait for a real list.
for _ in $(seq 1 60); do
  case "$(connect_api GET /connectors 2>/dev/null || true)" in '['*) break ;; esac
  sleep 3
done

echo "--- registering connectors"
connect_api POST /connectors kafka/connectors/debezium-postgres.json; echo
connect_api POST /connectors kafka/connectors/aiven-s3-sink.json; echo

for connector in demo-postgres-cdc rustfs-cdc-landing; do
  state=""
  for _ in $(seq 1 30); do
    status="$(connect_api GET "/connectors/$connector/status")"
    state="$(printf '%s' "$status" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(",".join([d["connector"]["state"]]+[t["state"] for t in d["tasks"]]))' 2>/dev/null || true)"
    case "$state" in RUNNING,RUNNING*) break ;; esac
    sleep 3
  done
  echo "$connector: $state"
  case "$state" in
    RUNNING,RUNNING*) ;;
    *) echo "FAIL: connector not running: $status" >&2; exit 1 ;;
  esac
done

echo "--- inserting a row; waiting for parquet in the landing bucket"
compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -c \
  "INSERT INTO pos.category_ref (category_code, category_name) VALUES ('CDC', 'cdc-smoke')"
for _ in $(seq 1 40); do
  files="$(landing_files || true)"
  if printf '%s' "$files" | grep -q 'demo.pos.category_ref/.*\.parquet'; then
    printf '%s\n' "$files" | grep 'demo.pos.category_ref'
    # The landing files must be readable parquet, not just present.
    rows="$(compose --profile tools run --rm --build -T etl python -c "
from conn import get_conn
con = get_conn()
rows = con.raw_sql(\"SELECT count(*) FROM read_parquet('s3://cdc-landing/cdc/demo.pos.category_ref/**/*.parquet')\").fetchone()[0]
print(rows)
" | tail -1 || true)"
    if ! [ "${rows:-0}" -ge 1 ] 2>/dev/null; then
      echo "FAIL: DuckDB could not read rows from the landing parquet: '$rows'" >&2
      exit 1
    fi
    echo "ok: DuckDB reads $rows CDC rows from parquet"
    echo "--- incremental: merging events into bronze, and through to the marts"
    run_etl() { compose --profile tools run --rm -T -e INGESTION_MODE=cdc etl "$@"; }
    clickhouse() {
      compose exec -T clickhouse clickhouse-client \
        --password "$(env_value CLICKHOUSE_PASSWORD)" -q "$1"
    }
    wait_for_events() { # table, condition on the landing files; flushes take up to a minute
      for _ in $(seq 1 40); do
        seen="$(run_etl python -c "
from conn import get_conn
print(get_conn().raw_sql(\"\"\"SELECT count(*) FROM read_parquet('s3://cdc-landing/cdc/demo.pos.$1/*/*.parquet') WHERE $2\"\"\").fetchone()[0])
" | tail -1 || true)"
        if [ "${seen:-0}" -ge 1 ] 2>/dev/null; then return 0; fi
        sleep 6
      done
      echo "FAIL: no landing event for $1 matching: $2" >&2
      connect_api GET /connectors/rustfs-cdc-landing/status >&2 || true
      return 1
    }
    # Everything the seed loaded has to land before bronze can match it.
    wait_for_events transaction_items "value.op = 'r' AND value.source.snapshot = 'last_in_data_collection'"
    wait_for_events customers "value.op = 'r' AND value.source.snapshot = 'last_in_data_collection'"
    run_etl python etl.py >/dev/null
    assert_rows() { # description, expected, query
      got="$(clickhouse "$3")"
      if [ "$got" != "$2" ]; then echo "FAIL: $1: expected '$2', got '$got'" >&2; exit 1; fi
      echo "ok: $1 = $got"
    }
    assert_rows "cdc bootstrap: fact rows" "6" "SELECT count() FROM marts.fact_sales"
    assert_rows "cdc bootstrap: revenue" "1656.5" "SELECT round(sum(line_amount), 2) FROM marts.fact_sales"

    compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo <<'SQL'
INSERT INTO pos.transactions (transaction_id, store_id, customer_id, transaction_date, status_code, payment_method)
VALUES (9002, 1, 1, '2026-09-01', 'C', 'CARD');
INSERT INTO pos.transaction_items (transaction_id, product_id, quantity, unit_price)
VALUES (9002, 1, 1, 100.00);
UPDATE pos.customers SET full_name = 'Renamed By CDC' WHERE customer_id = 1;
DELETE FROM pos.category_ref WHERE category_code = 'CDC';
SQL
    wait_for_events transaction_items "value.after.transaction_id = 9002"
    wait_for_events customers "value.op = 'u'"
    wait_for_events category_ref "value.op = 'd'"
    run_etl python etl.py >/dev/null
    assert_rows "an insert is merged" "7" "SELECT count() FROM marts.fact_sales"
    assert_rows "its revenue arrives" "1756.5" "SELECT round(sum(line_amount), 2) FROM marts.fact_sales"
    assert_rows "an update is merged" "Renamed By CDC" \
      "SELECT full_name FROM marts.dim_customer WHERE customer_id = 1 AND is_current"
    deleted="$(run_etl python -c "
from conn import get_conn
print(get_conn().raw_sql(\"\"\"SELECT count(*) FROM delta_scan('s3://lake/bronze/category_ref') WHERE category_code = 'CDC'\"\"\").fetchone()[0])
" 2>/dev/null | tail -1)"
    if [ "$deleted" != "0" ]; then echo "FAIL: the deleted row is still in bronze: '$deleted'" >&2; exit 1; fi
    echo "ok: a delete is merged"
    run_etl python etl.py >/dev/null
    assert_rows "a run with no new events changes nothing" "1756.5" \
      "SELECT round(sum(line_amount), 2) FROM marts.fact_sales"
    # A connector can say RUNNING while its task has failed. A source DELETE used to do
    # that to the sink, which stopped all landing.
    sink_task="$(connect_api GET /connectors/rustfs-cdc-landing/status \
      | python3 -c 'import json,sys; print(json.load(sys.stdin)["tasks"][0]["state"])')"
    if [ "$sink_task" != "RUNNING" ]; then echo "FAIL: sink task is $sink_task" >&2; exit 1; fi
    echo "ok: the sink task survived the delete"
    oom="$(docker ps -aq --filter "label=com.docker.compose.project=$PROJECT" \
      | xargs docker inspect --format '{{if .State.OOMKilled}}{{.Name}}{{end}}' | tr -d '\n')"
    if [ -n "$oom" ]; then
      echo "FAIL: containers killed for memory: $oom" >&2
      exit 1
    fi
    echo "ok: no container was killed for memory"
    echo "cdc smoke test passed"
    exit 0
  fi
  sleep 6
done

echo "FAIL: no parquet for demo.pos.category_ref after 4 minutes" >&2
echo "landing bucket: $(landing_files || true)" >&2
connect_api GET /connectors/rustfs-cdc-landing/status >&2 || true
compose logs --tail 40 connect >&2 || true
exit 1
