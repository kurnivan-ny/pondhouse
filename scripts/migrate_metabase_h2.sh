#!/usr/bin/env bash
# One-time move of Metabase's dashboards, users and settings from its old H2 file to
# Postgres. The H2 file is only read, and stays in the metabase-data volume.
#
# Usage:
#   bash scripts/migrate_metabase_h2.sh
#   bash scripts/migrate_metabase_h2.sh --replace-existing
#
# --replace-existing is for when Metabase already started on Postgres before this ran.
# It DELETES the `metabase` Postgres database first, including anything created there.
set -euo pipefail

cd "$(dirname "$0")/.."
H2_PATH=/metabase-data/metabase.db/metabase.db
# As the image's entrypoint runs Metabase. It mangles arguments, so bypass it here.
METABASE_UID=2000:2000

pg() { docker compose exec -T postgres sh -c "$1" sh "${@:2}"; }

docker compose up -d --wait postgres
docker compose run --rm metabase-db

if ! docker compose run --rm --no-deps -T --entrypoint /bin/sh metabase \
  -c "test -f $H2_PATH.mv.db" 2>/dev/null; then
  echo "No H2 database at $H2_PATH.mv.db in the metabase-data volume: nothing to migrate." >&2
  exit 1
fi

tables="$(pg 'psql -U "$POSTGRES_USER" -d metabase -Atc "SELECT count(*) FROM information_schema.tables WHERE table_schema = '"'"'public'"'"'"')"
if [ "$tables" != "0" ]; then
  if [ "${1:-}" != "--replace-existing" ]; then
    echo "The metabase Postgres database already has $tables tables, so nothing was changed." >&2
    echo "If Metabase started before this migration and holds nothing you need, run:" >&2
    echo "  bash scripts/migrate_metabase_h2.sh --replace-existing" >&2
    exit 1
  fi
fi

# H2 allows one process at a time, and Metabase must not write during the copy.
docker compose stop metabase

if [ "$tables" != "0" ]; then
  echo "--- recreating the metabase database"
  pg 'dropdb -U "$POSTGRES_USER" --force metabase'
  docker compose run --rm metabase-db
fi

echo "--- copying H2 -> Postgres (this can take a few minutes)"
docker compose run --rm --no-deps -T --user "$METABASE_UID" --entrypoint java metabase \
  -jar /app/metabase.jar load-from-h2 "$H2_PATH"

docker compose up -d metabase
echo "migrated. The H2 file is untouched in the metabase-data volume."
