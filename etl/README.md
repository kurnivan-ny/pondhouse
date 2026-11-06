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
| `ingestion` | `s3://lake/ingestion/*.parquet` | raw landing (Sling parquet) |
| `bronze` | `s3://lake/bronze/*` (Delta) | raw copy + `_ingested_at` |
| `silver` | `s3://lake/silver/*` (Delta) | **translate** (join refs → names), **cleanse**, **deduplicate**; `rejected_sales` keeps items that can't become a sale |
| `gold` | `s3://lake/gold/*` (Delta) | star schema: `dim_customer`, `dim_product`, `dim_store`, `dim_date`, `fact_sales` |
| `mart` | `s3://lake/mart/*` (Delta) | data marts: `daily_sales_mart`, `product_sales_mart`, `customer_sales_mart` |
| `marts` | ClickHouse | native tables loaded from gold + mart, swapped in atomically |

### Gold star schema

<p align="center">
  <img src="../docs/star-schema.svg" alt="fact_sales joins to dim_customer, dim_product, dim_store and dim_date on surrogate keys" width="88%">
</p>

## Layout

| File | Purpose |
|---|---|
| `sql/00_init.sql` | `pos` schema: source tables + reference lookups + seed data |
| `generate_data.py` | inserts new POS transactions (manual / scheduler) |
| `cdc.py` | incremental bronze: merges Debezium events when `INGESTION_MODE=cdc` |
| `ingest.py` | ingestion: runs the Sling replication in `ingestion/sling` |
| `bronze.py` | ingestion → **bronze** Delta (adds `_ingested_at`) |
| `transform.py` | **silver** (translate/cleanse/dedup) → **gold** (fact+dim) → **mart** |
| `serve.py` | loads native ClickHouse `marts` tables from the Delta gold + mart layers |
| `alerts.py` | sends failure alerts to `ALERT_WEBHOOK_URL`, used by Dagster |
| `checks.py` | data-quality checks, run by Dagster or by hand: `python checks.py gold` |
| `etl.py` | orchestrator: `ingest → bronze → transform → serve` (`--init` seeds) |
| `scheduler.py` | APScheduler cron: each cycle generates data + runs the ETL |
| `conn.py` | Ibis/DuckDB connection helper (S3 Delta) |
| `config.py` | Postgres + RustFS + cron config (env-overridable) |
| `Dockerfile` | `runtime` (pipeline only) and `dev` (adds tests, used by `docker compose run --rm etl ...`) targets |
| `tests/` | pytest suite (transforms, serving, checks, alerts, db helpers, backend parity) |

## Run it (Docker)

From the repository root. Step 0 is needed once; the rest is the pipeline.

```bash
# 0. core stack + buckets
docker compose up -d postgres rustfs clickhouse createbuckets

# 1. seed the POS source
docker compose exec -T postgres \
  psql -U pondhouse -d demo -v ON_ERROR_STOP=1 < etl/sql/00_init.sql

# 2. ingestion: postgres -> s3://lake/ingestion/*.parquet
docker compose --profile tools run --rm etl python ingest.py

# 3. bronze -> silver -> gold -> mart  (Delta on RustFS, computed with Ibis)
docker compose --profile tools run --rm etl python bronze.py
docker compose --profile tools run --rm etl python transform.py

# 4. load gold + mart into ClickHouse
docker compose --profile tools run --rm etl python serve.py
```

Steps 2–4 in one shot:

```bash
docker compose --profile tools run --rm etl python etl.py
```

Add more source rows and re-run at any time:

```bash
docker compose --profile tools run --rm etl python generate_data.py --rows 5
```

> The `etl` service joins the compose network, so it reaches the other services by
> name (`postgres`, `rustfs`, `clickhouse`). Running the scripts from your host
> instead requires `S3_ENDPOINT=http://localhost:9000` and `POSTGRES_PORT=5433`, plus the
> Sling CLI, with hosts in `ingestion/sling/env.yaml` changed to match.

## Data model

**Customers keep their history (SCD type 2).** The source only holds today's values, so each
run compares them with `silver.customer_history` and adds a row for anything that changed.
`dim_customer` has one row per version, with `version`, `valid_from`, `valid_to` and
`is_current`. Each sale and refund points to the version valid at midnight on its date, so old
sales keep the country the customer had then.

- `customer_history` is the one table that **can't be rebuilt** from the source. Back it up:
  `scripts/backup.sh` includes it.
- A version starts when a run first sees the change, so several changes between two runs
  count as one, and a sale dated the day of a change gets the older version.

**Keys are deterministic.** `customer_key` is `customer_id * 1000000 + version`, so a rebuild
gives the same keys. `product_key` and `store_key` are the surviving source ids, and
`date_key` is the date. Hash keys were rejected because Ibis has no MD5 for ClickHouse, which
would break the parity tests.

**`dim_date` is a full calendar**: every day of each year with a sale or refund, with
`quarter`, `month_name`, `day_name`, `is_weekend` and more.

**Refunds** come from `pos.refunds`. A refund must have a positive amount and, with the item's
earlier refunds, can't exceed what it sold for; others land in `silver.rejected_refunds`. Marts
keep gross figures unchanged and add `refund_amount` with `net_revenue` (or `total_refunded`
with `net_spent`). The daily mart counts a refund on the refund's date, so a day can have a
negative net.

## Incremental loads (CDC)

By default every run re-reads each table through Sling. With `INGESTION_MODE=cdc`, Sling is
skipped and bronze merges only new Debezium events from `s3://cdc-landing`, including deletes.
It needs the `cdc` profile running with both connectors registered (see the main README).
Set it in `.env`, or per run with `docker compose --profile tools run -e INGESTION_MODE=cdc ...`.

- Silver and later still rebuild fully, because their joins and dedup need all the rows.
- Events reach the landing bucket after a flush of up to about a minute.
- Switching over replays the topic, which holds Debezium's initial snapshot.
- Sling's own incremental mode isn't used: the source has no `updated_at`, so going by id
  would miss a transaction changing from pending to completed.
- Nothing alerts if a Connect task fails. Check `GET /connectors/<name>/status`, and look at
  the task's state, because the connector can say `RUNNING` while its task has `FAILED`.

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
in ClickHouse after a first run, a re-run, Dagster, a new source row, a lake outage, a customer change, refunds and an emptied source table. It uses its own compose
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
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `pondhouse` / `pondhouse-secret-change-me` | RustFS auth |
| `S3_ENDPOINT` | `http://localhost:9000` | RustFS endpoint (`http://rustfs:9000` in-network) |
| `S3_LAKE_BUCKET` | `lake` | lake bucket |
| `SLING_HOME` | `/opt/sling` in the images | Sling connections + replication |
| `CLICKHOUSE_HOST` / `CLICKHOUSE_PORT` | `localhost` / `8123` | serve step |
| `CLICKHOUSE_USER` / `CLICKHOUSE_PASSWORD` | `default` / `pondhouse` | ClickHouse auth |
| `ETL_SCHEDULE_CRON` | `*/5 * * * *` | scheduler cadence |
| `ALERT_WEBHOOK_URL` | *(empty)* | Dagster failure alerts |
| `INGESTION_MODE` | `snapshot` | `snapshot` (Sling) or `cdc` (merge Debezium events) |

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
- **An emptied source table lands as empty.** `SLING_ALLOW_EMPTY` makes Sling replace its
  parquet; otherwise the deleted rows would keep being served. The `has_rows` check then
  fails and sends an alert.
- **Only ingestion touches Postgres.** Bronze/silver/gold/mart read only the lake, so
  they still work when the source database is down.
- **Serving is atomic.** `serve.py` builds each table as `marts.{table}__new`, then swaps
  it in with `EXCHANGE TABLES`. Each table swaps on its own, so during a run some tables
  can be one load ahead of others.
- **Runs do not need the internet.** Sling, its DuckDB and the DuckDB extensions are baked
  into the images at build time.
