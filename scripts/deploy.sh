#!/usr/bin/env bash
# Deploy a published version: pull it, back up, start it, and go back to the previous
# version if it does not come up healthy. Run from a clone checked out at that version.
#
# Rollback restores the previous images and config, not data. If the new version changed
# a database schema, restore the backup taken here with scripts/restore.sh.
#
# Usage: bash scripts/deploy.sh <git commit>
set -euo pipefail

cd "$(dirname "$0")/.."
VERSION="${1:?usage: deploy.sh <git commit>}"
STATE_FILE=.deployed-version
WAIT_SECONDS="${DEPLOY_WAIT_SECONDS:-420}"
SERVICES=(rustfs postgres clickhouse metabase dagster-webserver dagster-daemon)

if [ ! -f .env ]; then
  echo "No .env here. See \"Deploying\" in README.md." >&2
  exit 1
fi

export COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-pondhouse}"
export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml:docker-compose.deploy.yml}"
export PONDHOUSE_VERSION="$VERSION"

start_stack() {
  docker compose up -d --wait --wait-timeout "$WAIT_SECONDS" "${SERVICES[@]}" \
    && docker compose run --rm createbuckets >/dev/null
}

PREVIOUS="$(cat "$STATE_FILE" 2>/dev/null || true)"

# Nothing has been touched if this fails, so the running version just stays up.
if [ -z "${DEPLOY_SKIP_PULL:-}" ]; then
  echo "--- pulling $VERSION"
  docker compose --profile tools pull --quiet "${SERVICES[@]}" etl
fi

if [ -n "$PREVIOUS" ]; then
  echo "--- backing up before replacing $PREVIOUS"
  # The backup runs against what is deployed now, not the version being installed.
  PONDHOUSE_VERSION="$PREVIOUS" bash scripts/backup.sh
fi

echo "--- starting $VERSION"
if start_stack; then
  echo "$VERSION" > "$STATE_FILE"
  echo "deployed $VERSION"
  exit 0
fi

echo "FAIL: $VERSION did not come up healthy" >&2
docker compose ps -a >&2 || true
if [ -z "$PREVIOUS" ]; then
  echo "This is the first deploy, so there is no earlier version to go back to." >&2
  exit 1
fi

echo "--- rolling back to $PREVIOUS" >&2
export PONDHOUSE_VERSION="$PREVIOUS"
if [ -z "${DEPLOY_SKIP_GIT:-}" ]; then
  git checkout --quiet --detach "$PREVIOUS"
fi
if start_stack; then
  echo "rolled back to $PREVIOUS; $VERSION was not deployed" >&2
else
  echo "FAIL: the rollback to $PREVIOUS is not healthy either. Check it by hand." >&2
fi
# Failing either way, so the workflow run shows that $VERSION did not deploy.
exit 1
