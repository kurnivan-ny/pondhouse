#!/usr/bin/env bash
# End-to-end check of the operational tooling: Metabase on Postgres and its H2 migration,
# backup and restore after real damage, failure alerts, and memory limits.
#
# Same isolation as smoke_test.sh: its own compose project, volumes and no published
# ports. Everything it creates is removed on exit.
set -euo pipefail

cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

# Picked up by every `docker compose` here, including in the scripts under test.
export COMPOSE_PROJECT_NAME=pondhouse-smoke-ops
export COMPOSE_FILE=docker-compose.yml:docker-compose.smoke.yml
BACKUP_DIR="$(mktemp -d)"
export BACKUP_DIR
CATCHER=pondhouse-smoke-ops-alert-catcher
export ALERT_WEBHOOK_URL="http://$CATCHER:8099/"
MB_H2=pondhouse-smoke-ops-metabase-h2

cleanup() {
  docker rm -f -v "$CATCHER" "$MB_H2" >/dev/null 2>&1 || true
  docker compose --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf "$BACKUP_DIR"
}
trap cleanup EXIT

env_value() { sed -n "s/^$1=//p" .env | head -1; }
POSTGRES_USER="$(env_value POSTGRES_USER)"
CLICKHOUSE_PASSWORD="$(env_value CLICKHOUSE_PASSWORD)"
EMAIL=smoke@example.com
PASSWORD=Sm0ke-Test-Zebra-9

clickhouse() {
  docker compose exec -T clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" -q "$1"
}
psql_db() { # database, statement
  docker compose exec -T postgres psql -qtA -U "$POSTGRES_USER" -d "$1" -c "$2"
}
assert_eq() { # description, expected, actual
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1: expected '$2', got '$3'" >&2
    exit 1
  fi
  echo "ok: $1 = $3"
}
assert_fails() { # description, command...
  local description="$1"; shift
  if "$@" >/dev/null 2>&1; then
    echo "FAIL: $description: it should have been refused" >&2
    exit 1
  fi
  echo "ok: $description was refused"
}
check_marts() { # stage label
  assert_eq "$1: fact_sales rows" "6" "$(clickhouse 'SELECT count() FROM marts.fact_sales')"
  assert_eq "$1: fact_sales revenue" "1656.5" \
    "$(clickhouse 'SELECT round(sum(line_amount), 2) FROM marts.fact_sales')"
}

# The Metabase API, from inside its container, since no ports are published.
mb_compose() { docker compose exec -T metabase curl -fsS "$@"; }
mb_h2() { docker exec "$MB_H2" curl -fsS "$@"; }
json_get() { python3 -c "import json,sys; print(json.load(sys.stdin)[\"$1\"])"; }
mb_login() {
  mb_compose -X POST -H 'Content-Type: application/json' http://localhost:3000/api/session \
    -d "{\"username\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | json_get id
}
mb_dashboard_names() { # session
  mb_compose -H "X-Metabase-Session: $1" http://localhost:3000/api/dashboard \
    | python3 -c 'import json,sys; print(",".join(sorted(d["name"] for d in json.load(sys.stdin))))'
}

echo "--- starting services and running the pipeline"
docker compose up -d --wait postgres clickhouse
docker compose up -d rustfs
docker compose run --rm createbuckets
docker compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 \
  < etl/sql/00_init.sql >/dev/null
docker compose --profile tools run --rm --build etl python etl.py >/dev/null
check_marts "pipeline"

echo "--- an old install: Metabase on H2, with a dashboard"
docker compose run -d --no-deps --name "$MB_H2" -e MB_DB_TYPE=h2 \
  -e MB_DB_FILE=/metabase-data/metabase.db metabase >/dev/null
for _ in $(seq 1 60); do
  if [ "$(mb_h2 http://localhost:3000/api/health 2>/dev/null)" = '{"status":"ok"}' ]; then break; fi
  sleep 5
done
TOKEN="$(mb_h2 http://localhost:3000/api/session/properties | json_get setup-token)"
SESSION="$(mb_h2 -X POST -H 'Content-Type: application/json' http://localhost:3000/api/setup \
  -d "{\"token\":\"$TOKEN\",\"user\":{\"email\":\"$EMAIL\",\"first_name\":\"Smoke\",\"last_name\":\"Test\",\"password\":\"$PASSWORD\",\"site_name\":\"smoke\"},\"prefs\":{\"site_name\":\"smoke\",\"allow_tracking\":false}}" \
  | json_get id)"
mb_h2 -X POST -H "X-Metabase-Session: $SESSION" -H 'Content-Type: application/json' \
  http://localhost:3000/api/dashboard -d '{"name":"Dashboard from H2"}' >/dev/null
docker stop -t 60 "$MB_H2" >/dev/null
docker rm -v "$MB_H2" >/dev/null

echo "--- upgrading: Metabase starts on Postgres, blank, before anyone migrates"
docker compose up -d --wait metabase
assert_eq "metabase stores its data in postgres" "t" \
  "$(psql_db metabase "SELECT count(*) > 0 FROM information_schema.tables WHERE table_name = 'report_dashboard'")"

echo "--- migrating the H2 dashboards"
assert_fails "migrating over a non-empty database without --replace-existing" \
  bash scripts/migrate_metabase_h2.sh
bash scripts/migrate_metabase_h2.sh --replace-existing >/dev/null
docker compose up -d --wait metabase
SESSION="$(mb_login)"
assert_eq "the H2 admin can log in, and sees the H2 dashboard" "yes" \
  "$(mb_dashboard_names "$SESSION" | grep -q 'Dashboard from H2' && echo yes || echo no)"
assert_eq "the H2 file is kept" "yes" \
  "$(docker compose exec -T metabase sh -c 'test -f /metabase-data/metabase.db/metabase.db.mv.db && echo yes || echo no')"

echo "--- backup, with rotation"
BACKUP_KEEP=2 bash scripts/backup.sh
sleep 1
BACKUP_KEEP=2 bash scripts/backup.sh
sleep 1
BACKUP_KEEP=2 bash scripts/backup.sh
assert_eq "rotation keeps the newest two" "2" \
  "$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
LATEST="$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d | sort | tail -1)"
for db in dagster demo metabase; do
  assert_eq "backup has $db" "yes" "$([ -s "$LATEST/postgres/$db.dump" ] && echo yes || echo no)"
done
assert_eq "backup has the Delta log" "yes" \
  "$([ -d "$LATEST/buckets/lake/gold/fact_sales/_delta_log" ] && echo yes || echo no)"

echo "--- damage: source rows, the lake, the marts and a dashboard all lost"
psql_db demo "DELETE FROM pos.transaction_items"
docker compose run --rm --no-deps -T --entrypoint /bin/sh createbuckets -c \
  'rc alias set rustfs http://rustfs:9000 "$S3_ACCESS_KEY" "$S3_SECRET_KEY" -q >/dev/null && rc object remove --recursive --force rustfs/lake' >/dev/null
clickhouse "TRUNCATE TABLE marts.fact_sales"
psql_db metabase "DELETE FROM report_dashboard WHERE name = 'Dashboard from H2'"
assert_eq "the damage is real" "0" "$(clickhouse 'SELECT count() FROM marts.fact_sales')"

echo "--- restore"
assert_fails "restoring without --yes" bash scripts/restore.sh "$LATEST"
bash scripts/restore.sh "$LATEST" --yes
docker compose --profile tools run --rm etl python serve.py >/dev/null
check_marts "restored lake"
assert_eq "source rows restored" "t" \
  "$(psql_db demo 'SELECT count(*) > 0 FROM pos.transaction_items')"
docker compose --profile tools run --rm etl python etl.py >/dev/null
check_marts "pipeline after restore"
docker compose up -d --wait metabase
SESSION="$(mb_login)"
assert_eq "dashboard restored" "yes" \
  "$(mb_dashboard_names "$SESSION" | grep -q 'Dashboard from H2' && echo yes || echo no)"

echo "--- failure alerts reach a webhook"
docker compose --profile tools run -d --no-deps --name "$CATCHER" -T etl python -u -c '
from http.server import BaseHTTPRequestHandler, HTTPServer
class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        print("ALERT", self.rfile.read(int(self.headers["Content-Length"])).decode(), flush=True)
        self.send_response(200)
        self.end_headers()
    def log_message(self, *args):
        pass
HTTPServer(("", 8099), Handler).serve_forever()
' >/dev/null
docker compose up -d --build --wait dagster-webserver dagster-daemon
alerts_contain() { # text, seconds to wait
  for _ in $(seq 1 "$(($2 / 5))"); do
    if docker logs "$CATCHER" 2>&1 | grep -q "$1"; then return 0; fi
    sleep 5
  done
  return 1
}
in_dagster() {
  docker compose exec -T -w /opt/dagster -e PYTHONPATH=/opt/dagster dagster-webserver "$@"
}

# The sensors only report what happens after their first evaluation.
for _ in $(seq 1 24); do
  if [ "$(psql_db dagster "SELECT count(*) > 1 FROM job_ticks" 2>/dev/null)" = "t" ]; then break; fi
  sleep 5
done

# A failed check does not fail the run. Removing a table the gold check reads fails it.
docker compose run --rm --no-deps -T --entrypoint /bin/sh createbuckets -c \
  'rc alias set rustfs http://rustfs:9000 "$S3_ACCESS_KEY" "$S3_SECRET_KEY" -q >/dev/null && rc object remove --recursive --force rustfs/lake/silver/rejected_sales' >/dev/null
in_dagster dagster asset materialize --select 'gold/fact_sales' -m repo.definitions >/dev/null 2>&1
assert_eq "a run with a failed check sends an alert" "yes" \
  "$(alerts_contain 'succeeded with 1 failed checks: gold/fact_sales gold_fact_check' 180 && echo yes || echo no)"

# Launched through the workspace and the daemon, as the schedule does.
docker compose stop rustfs
in_dagster dagster job launch -w workspace.yaml -j pondhouse_pipeline >/dev/null 2>&1
assert_eq "a failed run sends an alert" "yes" \
  "$(alerts_contain 'of pondhouse_pipeline failed' 240 && echo yes || echo no)"
docker compose start rustfs
assert_eq "one outage sends one message, not one per check" "2" \
  "$(docker logs "$CATCHER" 2>&1 | grep -c '^ALERT')"

echo "--- memory limits"
oom="$(docker ps -aq --filter "label=com.docker.compose.project=$COMPOSE_PROJECT_NAME" \
  | xargs docker inspect --format '{{if .State.OOMKilled}}{{.Name}}{{end}}' | tr -d '\n')"
assert_eq "containers killed for memory" "" "$oom"

echo "ops smoke test passed"
