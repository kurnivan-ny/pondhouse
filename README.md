# pondhouse

A lean, fully open-source lakehouse platform. One Ibis expression runs on both
DuckDB (lake) and ClickHouse (serving) — no dbt, no Spark, no ZooKeeper, no paid tiers.

**Requirements:** Docker with Compose v2 (runs great on WSL2), ~6 GB RAM free if you
enable every profile.

## Architecture

<p align="center">
  <img src="docs/architecture.svg" alt="Postgres POS source flows through Sling ingestion into a Delta medallion on RustFS (bronze, silver, gold, mart) computed with Ibis on DuckDB, then served to ClickHouse and Metabase, orchestrated by Dagster" width="100%">
</p>

Alongside the batch path above, a CDC path streams changes:
`Debezium → Kafka (KRaft) → Aiven S3 sink → s3://cdc-landing/cdc/<topic>/<partition>/*.parquet`
(profile `cdc`). Monitoring (Beszel) and catalog/lineage (OpenMetadata) ship as
their own profiles.

### Services and host ports

<p align="center">
  <img src="docs/services.svg" alt="Core services RustFS, Postgres, ClickHouse, Metabase and Dagster with their host ports, plus the tools, cdc, monitoring and governance profiles" width="100%">
</p>

## Stack

| Layer | Tool | Version | Profile | Port | URL |
|---|---|---|---|---|---|
| Storage | RustFS (S3) | 1.0.4 | core | 9000 API / 9001 console | http://localhost:9001 |
| Streaming | Kafka (KRaft) | 4.0.0 | `cdc` | 29092 host / 9092 internal | — |
| CDC | Kafka Connect + Debezium + Aiven S3 sink | 3.7.0 / 3.4.3 | `cdc` | 8083 | REST API |
| Serving | ClickHouse | 26.3 | core | 8123 HTTP / 9009 native | http://localhost:8123/play |
| Viz | Metabase (on Postgres; ClickHouse driver bundled) | v0.65.2 | core | 3000 | http://localhost:3000 |
| Orchestration | Dagster | 1.13 | core | 3001 | http://localhost:3001 |
| Shared DB + demo source | Postgres (wal_level=logical) | 16 | core | 5433 → 5432 (`POSTGRES_HOST_PORT`) | — |
| Batch ingestion | Sling (in the `etl` and Dagster images) | see `uv.lock` | `tools` | — | `docker compose --profile tools run --rm etl python ingest.py` |
| Medallion ETL | Ibis + DuckDB + deltalake | see `uv.lock` | `tools` | — | `docker compose run --rm etl ...` |
| Monitoring | Beszel (hub + agent) | 0.21.1 | `monitoring` | 8090 | http://localhost:8090 |
| Governance | OpenMetadata + Elasticsearch | 1.13.3 / 9.3.0 | `governance` | 8585 / 8080 | http://localhost:8585 |

## Quickstart

```bash
cd ~/pondhouse
cp .env.example .env                       # first time only; then edit as needed

docker compose up -d                       # core: rustfs, clickhouse, postgres, metabase, dagster
docker compose --profile cdc up -d         # + Kafka + Kafka Connect (Debezium + Aiven sink)
docker compose --profile monitoring up -d  # + Beszel (hub + agent)
docker compose --profile governance up -d  # + OpenMetadata + Elasticsearch
```

All profiles combine freely, e.g. `docker compose --profile cdc --profile monitoring up -d`.

> **First boot order matters:** start the **core stack first** and let Postgres finish
> initializing — its one-shot init script (`orchestration/initdb/01_dbs.sh`) creates the
> `dagster`, `demo`, `openmetadata_db` and `airflow_db` databases. If you enable the
> `governance` profile only *after* the Postgres volume was already initialized, create
> those DBs manually (one-liner in the Governance section below).

## Repo layout

```
pondhouse/
├── docker-compose.yml      # the whole platform (core + 4 profiles)
├── .env                    # all credentials / config knobs (gitignored)
├── .env.example            # copy to .env; documents every setting
├── pyproject.toml, uv.lock # all Python dependencies, one locked set
├── docs/                   # architecture / star-schema / services diagrams (SVG)
├── ingestion/sling/        # batch ingestion: connections + replication YAMLs
├── etl/                    # demo medallion ETL: Sling (oltp->bronze) + Ibis (silver/gold/mart)
│   ├── Dockerfile          # the `etl` service image
│   └── tests/              # pytest: transforms, ingest modes, DuckDB↔ClickHouse parity
├── kafka/                  # Connect image (Debezium + Aiven sink) + connector JSONs
├── orchestration/          # Dagster: image, instance config, assets + asset checks
│   └── initdb/             # postgres bootstrap: dagster/demo/openmetadata/airflow DBs
├── scripts/                # end-to-end smoke tests (pipeline, CDC), also run in CI
└── serving/                # ClickHouse: bootstrap SQL and config
```

## Configuration (.env)

| Variable | Used by | Default |
|---|---|---|
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | RustFS, Aiven sink, Sling, DuckDB secret | `pondhouse` / `pondhouse-secret-change-me` |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` / `CLICKHOUSE_DB` | ClickHouse | `default` / `pondhouse` / `marts` |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Postgres superuser | `pondhouse` / `pondhouse` / `pondhouse` |
| `BIND_ADDR` | Interface all ports publish on | `127.0.0.1` |
| `ETL_SCHEDULE_CRON` | Dagster + APScheduler cadence | `*/5 * * * *` |
| `ALERT_WEBHOOK_URL` | Dagster failure alerts (Slack or Discord webhook) | *(empty: only logged)* |
| `INGESTION_MODE` | `snapshot` (Sling) or `cdc` (merge Debezium events) | `snapshot` |
| `BESZEL_AGENT_KEY` | Beszel agent auth | *(paste from Beszel UI)* |
| `OM_VERSION` | OpenMetadata + ingestion image tag | `1.13.3` |
| `OM_FERNET_KEY` | OpenMetadata secret key; **required** for `governance` | *(generate)* |
| `OM_DB_PASSWORD` / `AIRFLOW_DB_PASSWORD` / `AIRFLOW_ADMIN_PASSWORD` | OpenMetadata + Airflow | dev defaults |

Every port publishes on `127.0.0.1` only. Change the defaults before setting
`BIND_ADDR` to anything else. All credentials live in `.env`; nothing in the repo
holds a real secret.

### Upgrading an existing install

- Add the new settings from `.env.example` to your `.env`.
- Dagster now keeps its state in Postgres, as its config always intended. Earlier
  installs silently used SQLite in the `dagster-home` volume, so old run history
  does not carry over. Once you don't need it: `docker volume rm pondhouse_dagster-home`.
- Containers no longer have fixed names, so stop the old ones first:
  `docker compose down --remove-orphans`. Data volumes are kept.
- `orchestration/initdb/01_dbs.sh` runs only on a fresh Postgres volume, so existing
  database passwords stay as they are.
- Rebuild the images: `docker compose build`. Sling is now inside the `etl` and Dagster
  images, so remove the old container with `docker compose --profile tools down
  --remove-orphans`. `SLING_USER` is no longer used.
- `etl.py --ingest`/`--mode` and `bronze.py --source-format` are gone: Sling is the only
  ingestion path. Delta tables from the old custom path stay at `s3://lake/ingestion/{table}/`
  unused, and can be deleted once you don't need them.
- The next `serve.py` or Dagster run replaces each `marts.*` table with a native one. This
  leaves the lake data untouched, and table and column names stay the same.
- **The data model changed (customer history, refunds, calendar).** Before the next run,
  create the new source table by re-running the seed, which is safe on existing data:
  `docker compose exec -T postgres psql -U pondhouse -d demo -v ON_ERROR_STOP=1 < etl/sql/00_init.sql`.
  - Customer history starts at this run, since nothing recorded earlier changes.
  - `customer_key` values change, and `marts.dim_date` has new columns (`transaction_date` is
    gone; use `date_key`). Check Metabase questions that filter on these.
  - If CDC is running, update its connector, or the first source `DELETE` stops all landing:
    `curl -X PUT http://localhost:8083/connectors/demo-postgres-cdc/config -H "Content-Type: application/json" -d "$(python3 -c 'import json; print(json.dumps(json.load(open("kafka/connectors/debezium-postgres.json"))["config"]))')"`.
- Dagster now has one asset per table, so old assets such as `bronze_layer` stay in its
  history but are no longer built.
- **Metabase moved from its H2 file to Postgres.** To keep your dashboards, run this
  before starting the new Metabase:

  ```bash
  docker compose stop metabase
  bash scripts/migrate_metabase_h2.sh
  ```

  The H2 file stays in the `metabase-data` volume. If Metabase already started and shows
  its setup screen, nothing is lost: run the script with `--replace-existing`, which
  deletes the new `metabase` Postgres database first, including anything created there.
  Keep the volume until you've checked your dashboards.

## Backups

`scripts/backup.sh` saves every Postgres database (including Metabase's dashboards and
Dagster's history) and the `lake` and `cdc-landing` buckets to `backups/<timestamp>/`:

```bash
bash scripts/backup.sh                                 # keeps the newest 7
BACKUP_DIR=/mnt/d/pondhouse-backups BACKUP_KEEP=14 bash scripts/backup.sh
```

Once deployed, the `Backup` workflow runs this nightly (see [Deploying](#deploying)).
Otherwise, schedule it yourself, for example with cron:

```cron
0 2 * * * cd $HOME/pondhouse && bash scripts/backup.sh >> $HOME/pondhouse-backup.log 2>&1
```

- **Copy backups off this machine.** By default they sit on the same disk as the data.
- **Treat them as secrets.** They hold password hashes and Metabase's stored connection
  details. `backups/` is ignored by git and readable only by you.
- **Avoid running one during a pipeline run.** The script compares object counts and fails
  if the lake changed during the copy; run it again if so.
- A backup appears under its final name only after its dumps and object counts are verified.
- ClickHouse isn't backed up, because its marts are rebuilt from the lake.

To restore, which **replaces** the current databases and buckets:

```bash
bash scripts/restore.sh backups/<timestamp> --yes
docker compose --profile tools run --rm etl python serve.py   # reload the marts
```

Restoring drops any Debezium replication slot, and CDC creates a new one when it restarts.
Restore has been tested with the core databases, not with OpenMetadata's.

## Deploying

Once CI passes on `main`, the `Deploy` workflow publishes images to GHCR on GitHub's
runners, then a self-hosted runner on your machine deploys them with `scripts/deploy.sh`.
That script takes a backup, starts the new version, and goes back to the previous one if
it doesn't become healthy. Rollback restores images and config, not data: if a release
changed a database schema, use `scripts/restore.sh` with the backup it took. A separate
`Backup` workflow runs nightly.

Publishing starts as soon as this is on `main`. Deploying and backups stay off until you
finish the steps below.

### Setting it up (once)

**1. Protect the runner first.** This repo is public and the runner executes code on your
machine, with Docker access that amounts to root. A pull request from a fork can edit a
workflow to run on your runner, so GitHub's approval step is what stops it:

- Settings → Actions → General → *Approval for running fork pull request workflows*: choose
  **Require approval for all external contributors**.
- **Never approve a run from someone else's PR that changes `.github/workflows/`** or the
  scripts it runs, unless you've read the changes.
- Settings → Environments → New environment `production` → Deployment branches and tags:
  **Selected branches and tags**, add `main`.

**2. Make a clone just for deployment**, separate from the one you develop in:

```bash
git clone https://github.com/kurnivan-ny/pondhouse.git ~/pondhouse-prod
cd ~/pondhouse-prod
cp .env.example .env    # then change every password, and set ALERT_WEBHOOK_URL
```

Its stack is the compose project `pondhouse`, the same name `docker compose` gives a clone
called `pondhouse`. So don't run `docker compose up` in your development clone while it's
deployed: it would replace these containers. Use `COMPOSE_PROJECT_NAME=pondhouse-dev` there,
and stop that stack first, since the published ports clash. The smoke tests are safe.

**3. Install the runner.** At Settings → Actions → Runners → New self-hosted runner → Linux,
GitHub shows download commands and a short-lived token. Keep the token to yourself. When
configuring, add the label the workflows look for:

```bash
./config.sh --url https://github.com/kurnivan-ny/pondhouse --token <TOKEN> \
  --name wsl-laptop --labels pondhouse-deploy --unattended
sudo ./svc.sh install && sudo ./svc.sh start   # needs systemd enabled in WSL; or ./run.sh
```

Without `sudo`, run it as a systemd user service instead: a unit in
`~/.config/systemd/user/` with `ExecStart=%h/actions-runner/run.sh` and `Restart=always`, then
`systemctl --user enable --now <unit>` and `loginctl enable-linger $USER`.

Its user must be able to run `docker` and read the deployment clone. Your machine has to be
on for a deploy to happen: jobs wait in the queue for up to a day.

**4. Turn it on.** At Settings → Secrets and variables → Actions → Variables, add:

| Variable | Value |
|---|---|
| `DEPLOY_DIR` | full path of the clone, e.g. `/home/you/pondhouse-prod` |
| `DEPLOY_ENABLED` | `true` |

**5. Run it once**: Actions → Deploy → Run workflow, on `main`. Then, if the source database
is new, seed it from the deployment clone:

```bash
cd ~/pondhouse-prod
export COMPOSE_PROJECT_NAME=pondhouse   # without this, compose starts a second stack
export COMPOSE_FILE=docker-compose.yml:docker-compose.deploy.yml
export PONDHOUSE_VERSION="$(cat .deployed-version)"
docker compose exec -T postgres psql -U pondhouse -d demo -v ON_ERROR_STOP=1 < etl/sql/00_init.sql
docker compose --profile tools run --rm etl python etl.py
```

Dagster's schedule starts switched off, so turn on `pondhouse_pipeline`'s schedule in its UI.

To pause deploys, set `DEPLOY_ENABLED` to anything else. To go back to a version by hand, run
`git checkout --detach <commit> && bash scripts/deploy.sh <commit>` in the deployment clone.

## Failure alerts

Set `ALERT_WEBHOOK_URL` in `.env` to a Slack or Discord incoming webhook, then recreate
Dagster with `docker compose up -d dagster-webserver dagster-daemon`. Each run sends at
most one message: when it fails, or when it succeeds but data-quality checks failed.
Without a URL, alerts only go to the `dagster-daemon` log. Delivery depends on
`dagster-daemon` running, so nothing is sent if the whole stack is down.

## Memory limits

Every core and `cdc` service has a `mem_limit` in `docker-compose.yml`, set above its
measured peak on the demo data. Java services also have a capped heap. A service that
exceeds its limit is killed, so raise the limit as your data grows; the smoke tests
fail if any container was killed this way. The `monitoring` and `governance` profiles have
no limits yet, because they haven't been measured.

## CDC pipeline

Once the `cdc` profile is up, register the connectors:

```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  -d @kafka/connectors/debezium-postgres.json   # source: demo DB -> topics demo.*

curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" \
  -d @kafka/connectors/aiven-s3-sink.json       # sink: topics -> parquet on RustFS

# Credentials are read from the connect container's environment (${env:NAME}).
# manage:  curl http://localhost:8083/connectors/<name>/status
#          curl -X DELETE http://localhost:8083/connectors/<name>
```

Flow: Debezium → Kafka topics (`demo.<schema>.<table>`) → Aiven sink →
`s3://cdc-landing/cdc/<topic>/<partition>/<offset>.parquet`, readable with DuckDB.
`bash scripts/smoke_cdc.sh` proves the whole path end to end. The landing files are
not yet consumed by the medallion pipeline, which reads the batch ingestion layer.

The bundled Postgres doubles as a demo CDC source (database `demo`, `wal_level=logical`).

## Demo ETL (retail POS: Sling → Ibis → Delta → ClickHouse)

A self-contained retail medallion pipeline in `etl/`, using **Delta** tables on
RustFS and **Ibis** (DuckDB) for all transformations. The gold layer is a star
schema:

<p align="center">
  <img src="docs/star-schema.svg" alt="fact_sales joins to dim_customer, dim_product, dim_store and dim_date on surrogate keys" width="80%">
</p>

- **`pos`** — retail source in the `demo` DB (`stores`, `products`, `customers`,
  `transactions`, `transaction_items` + `category_ref`/`status_ref`/`country_ref`
  lookups), seeded by `etl/sql/00_init.sql`.
- **`bronze`** — raw **Delta** copy + `_ingested_at` (`etl/bronze.py`), from the Sling
  parquet landing (`pos.*` → `s3://lake/ingestion/{table}.parquet`).
- **`silver`** — **translate** (join lookups → names), **cleanse**, **deduplicate**;
  rows that can't become a sale or refund are kept in `rejected_sales` and `rejected_refunds`.
  `customer_history` records every version of every customer.
- **`gold`** — star schema (`dim_customer` with history, `dim_product`, `dim_store`, a
  full-calendar `dim_date`, `fact_sales`, `fact_refunds`).
- **`mart`** — data marts (`daily_sales_mart`, `product_sales_mart`, `customer_sales_mart`).
- **`marts`** — native ClickHouse tables loaded from gold + mart by `etl/serve.py`. Each
  one is built separately and swapped in atomically, so a failed load leaves the old data
  serving, and queries keep working if RustFS is down.

Everything runs in Docker — no local Python needed:

```bash
docker compose up -d postgres rustfs clickhouse createbuckets   # core services

# seed the POS source (once)
docker compose exec -T postgres \
  psql -U pondhouse -d demo -v ON_ERROR_STOP=1 < etl/sql/00_init.sql

# the whole pipeline: ingest -> bronze -> silver/gold/mart -> clickhouse
docker compose --profile tools run --rm etl python etl.py

# add source rows, then re-run
docker compose --profile tools run --rm etl python generate_data.py --rows 5

# tests — includes DuckDB vs ClickHouse parity
docker compose --profile tools run --rm -e CLICKHOUSE_HOST=clickhouse etl \
  python -m pytest tests/ -q

# end-to-end checks; isolated projects, safe next to a live stack
bash scripts/smoke_test.sh   # the pipeline, with exact revenue
bash scripts/smoke_ops.sh    # Metabase migration, backup and restore, alerts
bash scripts/smoke_deploy.sh # deploy, upgrade, rollback
```

Or run it as a **Dagster** asset graph (ingestion → bronze → transform → serve,
with data-quality asset checks from `etl/checks.py`) from http://localhost:3001, or headless:

```bash
docker compose up -d dagster-webserver dagster-daemon
docker compose exec -w /opt/dagster -e PYTHONPATH=/opt/dagster \
  dagster-webserver dagster asset materialize --select '*' -m repo.definitions
```

- The **scheduler** (`etl/scheduler.py`, APScheduler) inserts a new POS transaction
  each cycle, then runs the whole pipeline. Cadence is `ETL_SCHEDULE_CRON`.
- Full reference: **`etl/README.md`**. The same `pos` tables can also feed the CDC
  pipeline above.

### Querying the marts (Metabase, DBeaver, any JDBC client)

ClickHouse publishes HTTP on **8123** and the native protocol on **9009**
(container 9000 is remapped because RustFS owns 9000 on the host). DBeaver's
ClickHouse driver uses HTTP, so connect with:

| Setting | Value |
|---|---|
| Host / Port | `localhost` / **8123** |
| Database | `marts` |
| User / Password | `default` / `pondhouse` (`.env`) |

```bash
curl -s -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" http://localhost:8123/ \
  --data-binary 'SELECT * FROM marts.daily_sales_mart ORDER BY date_key FORMAT Pretty'
```

## Monitoring (Beszel)

One-time key setup:

```bash
docker compose --profile monitoring up -d   # hub on :8090, agent starts after key
```

1. Open http://localhost:8090 → create the admin account.
2. **Add System** → host `host.docker.internal`, port `45876`.
3. Copy the generated **public key** into `.env` as `BESZEL_AGENT_KEY=ssh-ed25519 ...`.
4. `docker compose --profile monitoring up -d` again — the agent connects.

The agent reads per-container Docker stats via the read-only `docker.sock` mount and
host stats via host networking.

## Governance (OpenMetadata)

Integrated in the main compose under the `governance` profile (adapted from the
official OpenMetadata 1.13.3 Postgres compose, wired to the shared Postgres):

```bash
docker compose --profile governance up -d
```

- UI: http://localhost:8585 — login `admin@open-metadata.org` / `admin`
- Airflow (ingestion pipelines): http://localhost:8080 — `admin` / `admin`
- Set `OM_FERNET_KEY` in `.env` first: `openssl rand -base64 32`
- Elasticsearch needs `vm.max_map_count=262144` on the host (one-time):
  `sudo sysctl -w vm.max_map_count=262144`
- First boot takes a few minutes while the `openmetadata-migrate` one-shot runs DB migrations.
- Then add services in the UI for ClickHouse, Metabase, Dagster, Kafka and the
  S3/Delta lake (RustFS) to get catalog + lineage.
- If the Postgres volume was initialized *before* the governance profile existed, create
  OM's databases manually:
  `docker compose exec postgres bash /docker-entrypoint-initdb.d/01_dbs.sh`
- Upgrade via `OM_VERSION` in `.env`.

## DBeaver (and other host clients)

`serving/config.d/listen.xml` sets `listen_host=0.0.0.0` so the published ports
reach the server from the host. Connect DBeaver via HTTP: host `localhost`, port
`8123`, database `marts`, user `default`, password from `.env`
(`CLICKHOUSE_PASSWORD`). Native TCP also available on port `9009`.

## Dependencies

All Python dependencies are in `pyproject.toml`, with exact versions in `uv.lock`, so
every image and CI job gets the same set. To change one:

```bash
docker run --rm -v "$PWD:/w" -w /w -u "$(id -u):$(id -g)" -e UV_CACHE_DIR=/tmp/uv \
  python:3.12-slim sh -c "pip install -q --target /tmp/u uv==0.13.7 && PYTHONPATH=/tmp/u python -m uv lock --upgrade-package duckdb"
```

Dependabot proposes weekly updates for Python, images and GitHub Actions; CI, including
the end-to-end smoke tests and a blocking vulnerability scan, decides if they are safe.

## Testing

```bash
# Unit tests; REQUIRE_CLICKHOUSE=1 makes the parity tests fail rather than skip
docker compose up -d --wait clickhouse
docker compose --profile tools run --rm --build -e REQUIRE_CLICKHOUSE=1 \
  -e CLICKHOUSE_HOST=clickhouse etl python -m pytest tests/ -q

# End to end. Each uses its own compose project and volumes, with no published ports, so
# it's safe next to a live stack and cleans up after itself.
bash scripts/smoke_test.sh    # pipeline, Dagster, customer changes, refunds, a lake outage
bash scripts/smoke_cdc.sh     # Postgres -> Kafka -> parquet -> incremental bronze
bash scripts/smoke_ops.sh     # Metabase migration, backup and restore, alerts
bash scripts/smoke_deploy.sh  # deploy, upgrade, rollback
```

The seed data must give exactly **6 fact rows and 1656.50 revenue**, until a test adds more.
The smoke tests fit in 8 GB of RAM, with `smoke_ops.sh` the heaviest because Metabase takes
about 1.4 GB. Other stacks running alongside can use that up, so check `free -m` first.
CI runs all of this, plus lint, image builds and scans. It's the real test, because a local
run can hide problems through cached images.

## Known issues

**Pinned on purpose** (Dependabot is told to leave these alone; remove both together):
- `sqlglot<30.18`: later versions read a `DROP`'s table from `tables` while Ibis 12.0 passes
  `this`, so Ibis renders `DROP VIEW IF EXISTS` with no name. The pipeline doesn't use that
  path, so smoke tests pass without the pin and only the unit tests catch it.
- `sqlalchemy<2.1`: 2.1 defaults to psycopg v3, which isn't installed, so Dagster fails on startup.

**Accepted security findings.** Trivy blocks CI on `etl` and `dagster`, but only reports two
things, as both are libraries inside the latest upstream releases. Recheck them when upgrading:
- The Sling binary (42 HIGH/CRITICAL). It runs only for ingestion and opens no ports.
- The `connect` image (76 HIGH/CRITICAL). It listens on localhost only.

**ClickHouse**
- The user in `serving/users.d/named_collections.xml` must match `CLICKHOUSE_USER`. Version 26.3
  also needs `show_named_collections_secrets`, which lets that user read the S3 credentials.
- `DateTime` silently clamps to 1970-2106, so the customer history's date limits stay inside it.
- Ibis has no MD5 for ClickHouse, so keys are arithmetic rather than hashes.

**Sling**
- Image tags need a `v` prefix: `v1.6.5` exists, `1.6.5` doesn't.
- The PyPI package fetches its binary, and Sling fetches DuckDB, on first use. The Dockerfiles
  bake both in, so runs need no internet.
- Without `SLING_ALLOW_EMPTY`, a source table that becomes empty keeps its old parquet. With no
  matching source tables at all, Sling exits 0 having done nothing.

**CDC**
- Debezium's delete tombstones crash the Aiven parquet sink, hence `tombstones.on.delete: false`.
- A connector can say `RUNNING` while its task has `FAILED`, so check the task's state.
- In the Aiven sink, `file.name.prefix` is ignored when a template is set, so `cdc/` is in
  `file.name.template`. The legacy `aws.s3.prefix` setting overrides the template.

**Metabase**
- `MB_DB_FILE=/metabase-data/metabase.db` creates a folder, so the H2 path is
  `/metabase-data/metabase.db/metabase.db`. The image's entrypoint also drops the argument of
  `load-from-h2`, so `scripts/migrate_metabase_h2.sh` runs `java` directly as uid 2000.

**Dagster**
- `dagster.yaml` must live in `$DAGSTER_HOME`; otherwise it silently uses SQLite.
- Run sensors only watch their own code location unless told otherwise, and a run from
  `dagster asset materialize -m` counts as a different one.
- `TRANSFORMS` in `etl/transform.py` declares each table's reads, and Dagster orders assets from
  it. A unit test fails if a declaration differs from what the code reads.

**Compose**
- Shell variables override `.env`. If a shell has `CLICKHOUSE_PASSWORD` exported, running
  `docker compose` there recreates ClickHouse with that password, while other services read
  the real one from `.env`, and serving fails with `AUTHENTICATION_FAILED`. Fix it by unsetting
  the variable and running `docker compose up -d clickhouse`. The deploy runner isn't affected.

**Tooling**
- Gitleaks scans commits, not the working tree, so run it after committing and before pushing.
- `docker manifest inspect` gave false negatives here. Test an image with a real `docker pull`,
  and retry IPv6 "network is unreachable" errors before deciding an image is gone.
- On Windows + WSL, run git from inside WSL: Windows git with `autocrlf=true` rewrites files to CRLF.
- JVM services need a heap cap below their `mem_limit`. Connect used 2.1 GB by default.

**Not verified**
- The `governance` and `monitoring` profiles have never been run, only their compose config.
- Connecting Metabase to the marts through its UI. Its image does bundle the ClickHouse driver.
- A real Slack or Discord webhook. Alerts are tested against a local test webhook.

## Overlapping components (by design)

- **Dagster vs OM's Airflow** — same category, different scope. The Airflow inside the
  `ingestion` container is OpenMetadata's internal pipeline engine (required by the OM
  server); never add your own DAGs there. Dagster is *your* orchestrator.
- **Sling vs Debezium** — complementary: polling-based batch vs log-based CDC.
- **DuckDB vs ClickHouse** — intentional: compute engine vs serving engine.
