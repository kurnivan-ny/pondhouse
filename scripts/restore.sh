#!/usr/bin/env bash
# Restore a backup made by scripts/backup.sh. This REPLACES every database in the backup
# and the contents of its buckets, so it does nothing without --yes.
#
# Usage: bash scripts/restore.sh backups/<timestamp> --yes
set -euo pipefail

cd "$(dirname "$0")/.."
BACKUP="${1:-}"
if [ -z "$BACKUP" ] || [ ! -f "$BACKUP/MANIFEST" ]; then
  echo "usage: restore.sh <backup directory> --yes  (no MANIFEST found in '$BACKUP')" >&2
  exit 2
fi
BACKUP="$(cd "$BACKUP" && pwd)"

if [ "${2:-}" != "--yes" ]; then
  echo "This replaces the current data with the backup:" >&2
  cat "$BACKUP/MANIFEST" >&2
  echo "Run it again with --yes to continue." >&2
  exit 2
fi

pg() { docker compose exec -T postgres sh -c "$1" sh "${@:2}"; }

docker compose up -d --wait postgres
docker compose up -d rustfs

# Anything connected to these databases has to stop first, and restart afterwards.
USERS=(metabase dagster-webserver dagster-daemon connect)
RUNNING="$(docker compose ps --status running --services)"
RESTART=()
for service in "${USERS[@]}"; do
  if echo "$RUNNING" | grep -qx "$service"; then RESTART+=("$service"); fi
done
if [ "${#RESTART[@]}" -gt 0 ]; then
  docker compose stop "${RESTART[@]}"
fi

echo "--- postgres"
# Roles first. Errors are expected for roles that already exist.
pg 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < "$BACKUP/postgres/globals.sql" >/dev/null 2>&1 || true
for dump in "$BACKUP"/postgres/*.dump; do
  db="$(basename "$dump" .dump)"
  # A Debezium replication slot would block the drop; CDC recreates it on restart.
  pg 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = '"'"'$1'"'"'" >/dev/null' "$db"
  # POSTGRES_DB is the connection used for everything else, so it is emptied in place.
  if [ "$db" = "$(pg 'echo "$POSTGRES_DB"')" ]; then
    pg 'psql -q -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"'
    pg 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --exit-on-error' < "$dump"
  else
    pg 'dropdb -U "$POSTGRES_USER" --force --if-exists "$1"' "$db"
    pg 'createdb -U "$POSTGRES_USER" "$1"' "$db"
    pg 'pg_restore -U "$POSTGRES_USER" -d "$1" --exit-on-error' "$db" < "$dump"
  fi
  echo "$db restored"
done

echo "--- buckets"
for path in "$BACKUP"/buckets/*/; do
  bucket="$(basename "$path")"
  docker compose run --rm --no-deps -T --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -v "$BACKUP/buckets:/backup:ro" --entrypoint /bin/sh createbuckets -c \
    "rc alias set rustfs http://rustfs:9000 \"\$S3_ACCESS_KEY\" \"\$S3_SECRET_KEY\" -q >/dev/null \
     && rc bucket create --ignore-existing rustfs/$bucket >/dev/null \
     && rc mirror --quiet --overwrite --remove /backup/$bucket rustfs/$bucket" >/dev/null
  echo "$bucket restored"
done

if [ "${#RESTART[@]}" -gt 0 ]; then
  docker compose up -d "${RESTART[@]}"
fi

echo "restored from $BACKUP"
echo "Now reload the ClickHouse marts from the restored lake:"
echo "  docker compose --profile tools run --rm etl python serve.py"
