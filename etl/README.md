# pondhouse demo ETL

A self-contained medallion pipeline running on the pondhouse stack with minimal
data duplication:

```
oltp (postgres) ──Sling──► bronze/*.parquet (RustFS) ──Ibis──► silver → gold → mart (RustFS)
```

Postgres only keeps the transactional source (`oltp`). Everything downstream lives on
the lake (RustFS/S3): Sling batch-loads `oltp` to the bronze landing zone, then Ibis
(DuckDB engine) computes the silver/gold/mart layers.

## Layout

| File | Purpose |
|---|---|
| `sql/00_init.sql` | creates the `oltp` schema + source tables + seed sample data |
| `generate_data.py` | inserts new sample orders into `oltp` (manual / scheduler) |
| `migrate.py` | **Sling** batch migration `oltp.*` → `s3://lake/bronze/{table}.parquet` |
| `transform.py` | **Ibis** transforms `silver → gold → mart`, read/write RustFS parquet |
| `etl.py` | orchestrator: `migrate` → `transform` (add `--init` to seed first) |
| `scheduler.py` | APScheduler cron: each cycle generates data + runs the ETL |
| `config.py` | Postgres + RustFS + cron config (env-overridable, defaults match `.env`) |
| `db.py` | psycopg2 helpers |

The Sling connection + replication live in `../ingestion/sling/`:

- `env.yaml` — `DEMO_PG` (postgres) and `RUSTFS` (s3) connections.
- `replications/demo-to-lake.yaml` — `oltp.*` → `bronze/{stream_table}.parquet`
  (full-refresh).

## Layers

| Layer | Location | What it is |
|---|---|---|
| `oltp` | postgres `demo` | `customers`, `products`, `orders`, `order_items` (source of truth) |
| `bronze` | `s3://lake/bronze/*.parquet` | raw landing, one file per table |
| `silver` | `s3://lake/silver/*.parquet` | cleaned/deduped: `customers`, `products`, `orders`, `sales` |
| `gold` | `s3://lake/gold/*.parquet` | `daily_sales`, `category_sales`, `customer_ltv` |
| `mart` | `s3://lake/mart/*.parquet` | star schema: `dim_customer`, `dim_product`, `dim_date`, `fact_sales` |

## Setup

```bash
cd ~/pondhouse/etl
uv venv .venv
uv pip install --python .venv/bin/python -r requirements.txt
```

Requires the core stack up (`docker compose up -d` from `~/pondhouse`). The `tools`
profile (Sling) is pulled/started automatically by `migrate.py` on first run.

## Usage

```bash
# seed oltp + run the full pipeline once
.venv/bin/python etl.py --init

# add sample orders manually
.venv/bin/python generate_data.py --rows 5

# run migrate + transform only
.venv/bin/python etl.py

# scheduler: one cycle, then run forever on cron
.venv/bin/python scheduler.py --once
.venv/bin/python scheduler.py
```

## Scheduler

`scheduler.py` uses APScheduler. Each cycle:

1. inserts one random order into `oltp.orders` + `oltp.order_items`,
2. runs Sling (`migrate.py`),
3. runs the Ibis transform (`transform.py`).

Cadence is a 5-field cron in `ETL_SCHEDULE_CRON` (default `*/5 * * * *`):

```bash
ETL_SCHEDULE_CRON="0 * * * *" .venv/bin/python scheduler.py   # hourly
```

## Config

All knobs are env vars with defaults matching `../.env`:

| Variable | Default | Used by |
|---|---|---|
| `POSTGRES_HOST` / `POSTGRES_PORT` | `localhost` / `5432` | source postgres |
| `ETL_DB` | `demo` | source database |
| `POSTGRES_USER` / `POSTGRES_PASSWORD` | `pondhouse` | postgres auth |
| `S3_ACCESS_KEY` / `S3_SECRET_KEY` | `pondhouse` / `pondhouse-secret-change-me` | RustFS auth |
| `S3_ENDPOINT` | `localhost:9000` | RustFS endpoint (use `rustfs:9000` inside the compose network) |
| `S3_LAKE_BUCKET` | `lake` | lake bucket |
| `ETL_SCHEDULE_CRON` | `*/5 * * * *` | scheduler cadence |
