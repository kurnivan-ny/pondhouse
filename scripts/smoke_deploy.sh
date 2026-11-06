#!/usr/bin/env bash
# End-to-end check of scripts/deploy.sh using locally tagged images: a first deploy, an
# upgrade with its backup, and a broken version that must roll back without losing data.
#
# Same isolation as smoke_test.sh: its own compose project, volumes and no published
# ports. Everything it creates is removed on exit.
set -euo pipefail

cd "$(dirname "$0")/.."
[ -f .env ] || cp .env.example .env

export COMPOSE_PROJECT_NAME=pondhouse-smoke-deploy
export COMPOSE_FILE=docker-compose.yml:docker-compose.smoke.yml:docker-compose.deploy.yml
export PONDHOUSE_IMAGE_PREFIX=pondhouse-smoke-deploy
export DEPLOY_SKIP_PULL=1 DEPLOY_SKIP_GIT=1 DEPLOY_WAIT_SECONDS=300
BACKUP_DIR="$(mktemp -d)"
export BACKUP_DIR
# The compose file requires a version even for the commands that don't use it.
export PONDHOUSE_VERSION=v1

cleanup() {
  docker compose --profile tools down -v --remove-orphans >/dev/null 2>&1 || true
  docker image rm -f "$PONDHOUSE_IMAGE_PREFIX-dagster:v1" "$PONDHOUSE_IMAGE_PREFIX-dagster:v2" \
    "$PONDHOUSE_IMAGE_PREFIX-dagster:broken" "$PONDHOUSE_IMAGE_PREFIX-etl:v1" \
    "$PONDHOUSE_IMAGE_PREFIX-etl:v2" "$PONDHOUSE_IMAGE_PREFIX-etl:broken" >/dev/null 2>&1 || true
  rm -f .deployed-version
  rm -rf "$BACKUP_DIR"
}
if [ -f .deployed-version ]; then
  echo "FAIL: .deployed-version exists, so this looks like a real deployment clone" >&2
  exit 1
fi
trap cleanup EXIT

env_value() { sed -n "s/^$1=//p" .env | head -1; }
POSTGRES_USER="$(env_value POSTGRES_USER)"
CLICKHOUSE_PASSWORD="$(env_value CLICKHOUSE_PASSWORD)"

assert_eq() { # description, expected, actual
  if [ "$2" != "$3" ]; then
    echo "FAIL: $1: expected '$2', got '$3'" >&2
    exit 1
  fi
  echo "ok: $1 = $3"
}
running_image() {
  docker inspect --format '{{.Config.Image}}' "$(docker compose ps -q dagster-webserver)"
}
marts_rows() {
  docker compose exec -T clickhouse clickhouse-client --password "$CLICKHOUSE_PASSWORD" \
    -q 'SELECT count() FROM marts.fact_sales'
}

echo "--- building versions"
docker build -q -f orchestration/Dockerfile -t "$PONDHOUSE_IMAGE_PREFIX-dagster:v1" . >/dev/null
docker build -q -f etl/Dockerfile --target runtime -t "$PONDHOUSE_IMAGE_PREFIX-etl:v1" . >/dev/null
docker tag "$PONDHOUSE_IMAGE_PREFIX-dagster:v1" "$PONDHOUSE_IMAGE_PREFIX-dagster:v2"
docker tag "$PONDHOUSE_IMAGE_PREFIX-etl:v1" "$PONDHOUSE_IMAGE_PREFIX-etl:v2"
# A version whose Dagster starts and then dies, like a bad release would.
printf 'FROM %s\nENTRYPOINT ["sh", "-c", "exit 1"]\n' "$PONDHOUSE_IMAGE_PREFIX-dagster:v1" \
  | docker build -q -t "$PONDHOUSE_IMAGE_PREFIX-dagster:broken" - >/dev/null
docker tag "$PONDHOUSE_IMAGE_PREFIX-etl:v1" "$PONDHOUSE_IMAGE_PREFIX-etl:broken"

echo "--- first deploy"
bash scripts/deploy.sh v1
assert_eq "recorded version" "v1" "$(cat .deployed-version)"
assert_eq "no backup on a first deploy" "0" \
  "$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
docker compose exec -T postgres psql -q -U "$POSTGRES_USER" -d demo -v ON_ERROR_STOP=1 \
  < etl/sql/00_init.sql >/dev/null
docker compose --profile tools run --rm etl python etl.py >/dev/null
assert_eq "the published etl image runs the pipeline" "6" "$(marts_rows)"
assert_eq "dagster loads" "yes" "$(docker compose exec -T -w /opt/dagster -e PYTHONPATH=/opt/dagster \
  dagster-webserver python -c 'import repo.definitions; print("yes")' 2>/dev/null | tail -1)"

echo "--- upgrade"
bash scripts/deploy.sh v2
assert_eq "recorded version" "v2" "$(cat .deployed-version)"
assert_eq "running version" "$PONDHOUSE_IMAGE_PREFIX-dagster:v2" "$(PONDHOUSE_VERSION=v2 running_image)"
assert_eq "the upgrade took a backup first" "1" \
  "$(find "$BACKUP_DIR" -mindepth 1 -maxdepth 1 -type d | wc -l | tr -d ' ')"
assert_eq "data survives an upgrade" "6" "$(marts_rows)"

echo "--- a broken version must fail and roll back"
if DEPLOY_WAIT_SECONDS=90 bash scripts/deploy.sh broken; then
  echo "FAIL: deploying a broken version reported success" >&2
  exit 1
fi
echo "ok: deploying the broken version failed"
assert_eq "recorded version is unchanged" "v2" "$(cat .deployed-version)"
assert_eq "the previous version is running" "$PONDHOUSE_IMAGE_PREFIX-dagster:v2" \
  "$(PONDHOUSE_VERSION=v2 running_image)"
assert_eq "dagster is healthy again" "healthy" \
  "$(docker inspect --format '{{.State.Health.Status}}' "$(PONDHOUSE_VERSION=v2 docker compose ps -q dagster-webserver)")"
assert_eq "data survives a failed deploy" "6" "$(marts_rows)"

echo "deploy smoke test passed"
