# pondhouse demo ETL — retail POS medallion

A self-contained retail data pipeline on the pondhouse stack: **Delta** tables on
RustFS, all transformations written **once in Ibis** and executed on DuckDB —
then served to ClickHouse for BI tools.

Everything below runs **inside Docker**. No local Python install is required.

<p align="center">
  <img src="../docs/architecture.svg" alt="Postgres POS to Sling ingestion to a Delta medallion on RustFS, computed with Ibis on DuckDB, served to ClickHouse and Metabase, orchestrated by Dagster" width="100%">
</p>

Postgres only holds the POS source (`pos` schema, simulating a POS / SAP HANA
system with coded columns that need translation). Everything downstream is Delta
on RustFS, computed with Ibis, and served to ClickHouse.

## Layers

| Layer | Location | What it is |
|---|---|---|
| `pos` | postgres `demo` | source of truth: `stores`, `products`, `customers`, `transactions`, `transaction_items` + `category_ref`, `status_ref`, `country_ref` |
| `ingestion` | `s3://lake/ingestion/*.parquet` | raw landing (Sling parquet, or custom Delta) |
| `bronze` | `s3://lake/bronze/*` (Delta) | raw copy + `_ingested_at` |
| `silver` | `s3://lake/silver/*` (Delta) | **translate** (join refs → names), **cleanse**, **deduplicate**; `rejected_sales` keeps items that can't become a sale |
| `gold` | `s3://lake/gold/*` (Delta) | star schema: `dim_customer`, `dim_product`, `dim_store`, `dim_date`, `fact_sales` |
| `mart` | `s3://lake/mart/*` (Delta) | data marts: `daily_sales_mart`, `product_sales_mart`, `customer_sales_mart` |
| `marts` | ClickHouse | serving tables (`DeltaLake` engine) reading gold + mart from RustFS |

### Gold star schema

<p align="center">
  <img src="../docs/star-schema.svg" alt="fact_sales joins to dim_customer, dim_product, dim_store and dim_date on surrogate keys" width="88%">
</p>

## Layout

| File | Purpose |
|---|---|
| `sql/00_init.sql` | `pos` schema: source tables + reference lookups + seed data |
| `generate_data.py` | inserts new POS transactions (manual / scheduler) |
| `ingest.py` | ingestion: Sling (default) or custom Delta ingestion (`overwrite`/`append`/`merge`) |
| `bronze.py` | ingestion → **bronze** Delta (adds `_ingested_at`) |
| `transform.py` | **silver** (translate/cleanse/dedup) → **gold** (fact+dim) → **mart** |
| `serve.py` | creates ClickHouse `marts` tables over the Delta gold + mart layers |
| `checks.py` | data-quality checks, run by Dagster or by hand: `python checks.py gold` |
| `etl.py` | orchestrator: `ingest → bronze → transform → serve` (`--init` seeds) |
| `scheduler.py` | APScheduler cron: each cycle generates data + runs the ETL |
| `conn.py` | Ibis/DuckDB connection helper (S3 Delta, optional Postgres attach) |
| `config.py` | Postgres + RustFS + cron config (env-overridable) |
| `Dockerfile` | `runtime` (pipeline only) and `dev` (adds tests, used by `docker compose run --rm etl ...`) targets |
| `tests/` | pytest suite (transforms, ingest modes, db helpers, backend parity) |

## Run it (Docker)

From the repository root. Step 0 is needed once; the rest is the pipeline.

```bash
# 0. core stack + buckets
docker compose up -d postgres rustfs clickhouse createbuckets

# 1. seed the POS source
docker compose exec -T postgres \
  psql -U pondhouse -d demo -v ON_ERROR_STOP=1 < etl/sql/00_init.sql

# 2. ingestion: postgres -> s3://lake/ingestion/*.parquet
docker compose --profile tools up -d sling
docker compose --profile tools exec -T sling \
  sling run --home-dir /sling -r /sling/replications/demo-to-lake.yaml

# 3. bronze -> silver -> gold -> mart  (Delta on RustFS, computed with Ibis)
docker compose --profile tools run --rm etl python bronze.py
docker compose --profile tools run --rm etl python transform.py

# 4. expose gold + mart to ClickHouse
docker compose --profile tools run --rm etl python serve.py
```

Steps 2–4 in one shot:

```bash
docker compose --profile tools run --rm etl python etl.py
```

`etl.py` picks the ingestion method automatically (`--ingest auto`, the default):
Sling when the docker CLI is reachable — which is the case on the host — and the
equivalent Ibis/DuckDB ingestion inside the container, where there is no docker
socket. Force one with `--ingest sling` or `--ingest custom`.

Add more source rows and re-run at any time:

```bash
docker compose --profile tools run --rm etl python generate_data.py --rows 5
```

> The `etl` service joins the compose network, so it reaches the other services by
> name (`postgres`, `rustfs`, `clickhouse`). Running the scripts from your host
> instead requires `S3_ENDPOINT=http://localhost:9000` and `POSTGRES_PORT=5433`.

## Ingestion modes

`ingest.py --method custom --mode {overwrite,append,merge}` writes the source to a
Delta `ingestion` layer:

- **overwrite** — replace the ingestion Delta table (default)
- **append** — append every source row stamped with `_loaded_at`. The ingestion
  layer keeps a history of copies; silver keeps only the newest copy of each key
- **merge** — upsert on the table's primary key, so re-runs are idempotent

```bash
docker compose --profile tools run --rm etl python ingest.py --method custom --mode merge
```

## Querying the result

ClickHouse serves the gold + mart layers as `marts.*`:

```bash
curl -s -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" http://localhost:8123/ \
  --data-binary 'SELECT * FROM marts.daily_sales_mart ORDER BY date_key FORMAT Pretty'
```

### DBeaver / any JDBC client

The ClickHouse HTTP port is published on the host, so a local client connects with:

| Setting | Value |
|---|---|
| Driver | ClickHouse |
| Host | `localhost` |
| Port | **8123** (HTTP — the default for DBeaver's driver) |
| Database | `marts` |
| User | `default` |
| Password | `pondhouse` (from `.env`) |

The native protocol is also published on **9009** (mapped from the container's
9000, because RustFS already uses 9000 on the host). Use 8123 unless your client
specifically needs the native driver. Metabase reaches ClickHouse in-network at
`clickhouse:8123`.

Postgres is published on **5433** by default (`POSTGRES_HOST_PORT` in `.env`) so
it does not collide with another local Postgres.

## Tests

```bash
docker compose --profile tools run --rm etl python -m pytest tests/ -q
```

The suite covers the silver/gold/mart logic, the ingestion write modes, the
psycopg2 helpers, and **backend parity**: the same Ibis expressions are executed
on DuckDB *and* ClickHouse and the results compared row-for-row. The ClickHouse
tests skip when no server is reachable, unless `REQUIRE_CLICKHOUSE=1` is set, which
makes them fail instead. CI sets it, so parity can't silently stop being tested.

To include them, point the tests at the running ClickHouse:

```bash
docker compose --profile tools run --rm -e CLICKHOUSE_HOST=clickhouse etl \
  python -m pytest tests/ -q
```

`scripts/smoke_test.sh` runs the real pipeline end to end and asserts exact revenue
in ClickHouse after overwrite, repeated append, and merge. It uses its own compose
project, volumes and no published ports, so it is safe next to a live stack.

## Scheduler

`scheduler.py` uses APScheduler. Each cycle inserts a random POS transaction, then
runs the full ETL. Cadence is a 5-field cron in `ETL_SCHEDULE_CRON` (default
`*/5 * * * *`). In production, prefer the Dagster schedule (`orchestration/`),
which materializes the same steps as an asset graph.

## Config

All knobs are env vars with defaults matching `../.env`:

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | source postgres |
| `POSTGRES_HOST_PORT` | `5433` | host port mapping (compose only) |
| `ETL_DB` | `demo` | source database |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `pondhouse` | postgres auth |
| `ETL_ATTACH_PG` | `0` | attach Postgres to DuckDB (only ingestion needs it) |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `pondhouse` / `pondhouse-secret-change-me` | RustFS auth |
| `S3_ENDPOINT` | `http://localhost:9000` | RustFS endpoint (`http://rustfs:9000` in-network) |
| `S3_LAKE_BUCKET` | `lake` | lake bucket |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` | `localhost` / `8123` | serve step |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / `pondhouse` | ClickHouse auth |
| `ETL_SCHEDULE_CRON` | `*/5 * * * *` | scheduler cadence |

## Notes on correctness

- **Revenue is not lost or multiplied by silver.** Bronze rows are deduplicated on
  their primary key. Sales of a duplicate customer or product move to the surviving
  record, and keep the price actually charged. Unknown country or category codes are
  labelled `Unknown` rather than dropped. Completed items that still can't become a
  sale land in `silver.rejected_sales`, so sales + rejected reconcile with the source.
- **Overwrites are vacuumed.** A Delta overwrite only tombstones the previous data
  files; they stay in the bucket and would pile up on a frequent schedule, so
  `write_delta` vacuums after each write. Older ClickHouse versions also failed on these
  stale files; ClickHouse 26.3 follows the transaction log and reads correctly without it.
- **`_ingested_at` is batch-level.** `ibis.now()` is transaction-constant, so every
  row of a given run shares one load timestamp — intended for a batch load stamp.
- **Asset checks are real.** Each layer has row counts; gold also checks uniqueness,
  foreign keys and revenue against silver; serving compares ClickHouse row counts
  with Delta. A failed check is reported in Dagster but does not stop the run.
- **Only ingestion touches Postgres.** The lake steps run with no Postgres attach,
  so bronze/silver/gold/mart still work when the source database is down.
