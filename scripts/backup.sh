#!/usr/bin/env bash
# Back up every Postgres database and the RustFS buckets to BACKUP_DIR/<timestamp>/.
#
# ClickHouse is not backed up: its marts are rebuilt from the lake by etl/serve.py.
# A backup only appears under its final name once it has been verified. The newest
# BACKUP_KEEP backups are kept. See "Backups" in README.md.
#
# Usage: bash scripts/backup.sh
set -euo pipefail

cd "$(dirname "$0")/.."
BACKUP_ROOT="${BACKUP_DIR:-$PWD/backups}"
KEEP="${BACKUP_KEEP:-7}"
BUCKETS=(lake cdc-landing)

# Dumps include password hashes and Metabase's stored connection details.
umask 077
mkdir -p "$BACKUP_ROOT"
BACKUP_ROOT="$(cd "$BACKUP_ROOT" && pwd)"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
WORK="$BACKUP_ROOT/.incomplete-$STAMP"
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$WORK/postgres" "$WORK/buckets"

pg() { docker compose exec -T postgres sh -c "$1" sh "${@:2}"; }

rc_run() { # Runs as the host user, so the files can be rotated without root.
  docker compose run --rm --no-deps -T --user "$(id -u):$(id -g)" -e HOME=/tmp \
    -v "$WORK/buckets:/backup" --entrypoint /bin/sh createbuckets -c \
    'rc alias set rustfs http://rustfs:9000 "$S3_ACCESS_KEY" "$S3_SECRET_KEY" -q >/dev/null && '"$1"
}

docker compose up -d --wait postgres
docker compose up -d rustfs

echo "--- postgres"
pg 'pg_dumpall -U "$POSTGRES_USER" --globals-only' > "$WORK/postgres/globals.sql"
DATABASES="$(pg 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Atc "SELECT datname FROM pg_database WHERE NOT datistemplate AND datname <> '"'"'postgres'"'"' ORDER BY 1"')"
for db in $DATABASES; do
  pg 'pg_dump -U "$POSTGRES_USER" -Fc "$1"' "$db" > "$WORK/postgres/$db.dump"
  # Reading the table of contents proves the archive is complete and readable.
  docker compose exec -T postgres pg_restore --list < "$WORK/postgres/$db.dump" >/dev/null
  echo "$db: $(du -h "$WORK/postgres/$db.dump" | cut -f1)"
done

echo "--- buckets"
for bucket in "${BUCKETS[@]}"; do
  mkdir -p "$WORK/buckets/$bucket"
  rc_run "rc mirror --quiet rustfs/$bucket /backup/$bucket" >/dev/null
  # Counted at the source after mirroring, so a pipeline that ran during the copy can
  # fail this check. Run the backup again if it does.
  source_count="$(rc_run "rc object list --recursive --json rustfs/$bucket" | python3 -c '
import json, sys
listing = json.load(sys.stdin)
if listing.get("truncated"):
    sys.exit("the object listing was truncated")
print(sum(1 for item in listing["items"] if not str(item.get("key", "")).endswith("/")))
')"
  copied_count="$(find "$WORK/buckets/$bucket" -type f | wc -l | tr -d ' ')"
  if [ "$source_count" != "$copied_count" ]; then
    echo "FAIL: $bucket has $source_count objects but $copied_count were copied" >&2
    exit 1
  fi
  echo "$bucket: $copied_count objects"
done

{
  echo "created_utc=$STAMP"
  echo "git_commit=$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "databases=$(echo "$DATABASES" | tr '\n' ' ')"
  echo "buckets=${BUCKETS[*]}"
} > "$WORK/MANIFEST"

mv "$WORK" "$BACKUP_ROOT/$STAMP"
trap - EXIT
echo "backup written to $BACKUP_ROOT/$STAMP"

# Rotate. Timestamps sort by age; only names shaped like a backup are ever removed.
find "$BACKUP_ROOT" -mindepth 1 -maxdepth 1 -type d -name '20[0-9][0-9][0-1][0-9]*T*Z' \
  | sort -r | tail -n "+$((KEEP + 1))" | while read -r old; do
    echo "removing old backup $(basename "$old")"
    rm -rf "$old"
  done
