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
