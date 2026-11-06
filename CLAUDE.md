# pondhouse

Open-source lakehouse in Docker Compose: Postgres (POS source) -> Sling or custom Ibis
ingestion -> Delta medallion on RustFS (bronze/silver/gold/mart, computed with Ibis on
DuckDB) -> ClickHouse -> Metabase, orchestrated by Dagster. A separate CDC path runs
Debezium -> Kafka -> Aiven S3 sink -> `s3://cdc-landing/cdc/`. See `README.md` and
`etl/README.md`.

## Working agreements

- One branch and PR per stage, never commit directly to `main`. The owner reviews and
  merges each PR in the GitHub UI.
- Update the docs in the same PR as the change.
- Reproduce a suspected bug before fixing it, and add a regression test.
- Report results honestly: say what was and wasn't run. CI on a clean machine is the
  real test, because local runs can hide problems through cached images.
- Before deleting Docker volumes or other data, list them and ask. Volumes may hold
  real data, including the unrelated `fithub` project on the original machine.

## Testing

```bash
cp .env.example .env                 # first time only

# Unit tests; REQUIRE_CLICKHOUSE=1 makes parity tests fail rather than skip
docker compose up -d --wait clickhouse
docker compose --profile tools run --rm --build -e REQUIRE_CLICKHOUSE=1 \
  -e CLICKHOUSE_HOST=clickhouse etl python -m pytest tests/ -q

# End to end. Each uses its own compose project, volumes and no published ports,
# so they are safe next to a live stack, and they clean up after themselves.
bash scripts/smoke_test.sh           # overwrite, append x2, merge, Sling, checks, Dagster
bash scripts/smoke_cdc.sh            # row in Postgres -> readable parquet on RustFS
```

The seed data must give exactly **6 fact rows and 1656.50 revenue** at every stage of
`smoke_test.sh`. Smoke tests are the heaviest step: allow about 8 GB of free RAM.
CI (`.github/workflows/ci.yml`) runs lint, tests, both smoke tests, image builds and scans.

## Gotchas already learned

- On Windows + WSL, run git from inside WSL. Windows git with `autocrlf=true` rewrites
  working files to CRLF. `.gitattributes` on the stage 3 branch guards against this.
- `docker manifest inspect` gave false negatives here. Test image availability with a
  real `docker pull`. Pulls also failed at times from IPv6 "network is unreachable";
  retry before concluding an image is gone.
- `minio/mc` can no longer be pulled, so buckets are created with `rustfs/rc`.
- SQLAlchemy 2.1 changes the default Postgres driver to psycopg v3, which broke the
  NOTIFY statements in dagster-postgres 0.23. Stage 2 pins `<2.1`. Stage 3 drops the
  pin, so confirm dagster-postgres 0.29 works with the locked 2.1.8.
- `dagster.yaml` must live in `$DAGSTER_HOME`; otherwise Dagster silently uses SQLite.
- ClickHouse's named collection needs `named_collection_control` for the user
  (`serving/users.d/named_collections.xml`), which must match `CLICKHOUSE_USER`.
- ClickHouse rejects a Delta table whose files have different schemas, so `etl/bronze.py`
  casts to one schema (`SOURCE_SCHEMAS`) whichever ingestion path ran.
- Aiven sink: `file.name.prefix` is ignored when a template is set, so `cdc/` is in
  `file.name.template`. The legacy `aws.s3.prefix` setting overrides the template.
- GitHub runners use a different uid than 1000, hence `SLING_USER` in `smoke_test.sh`.
- Unverified: whether Metabase needs a separate ClickHouse driver plugin.
- Not run yet: the `governance` (OpenMetadata) and `monitoring` (Beszel) profiles. Only
  their compose config has been validated.
- A `deltalake` warning appears on many writes ("Attempting to write a transaction N
  but the underlying table has been updated to N"). Data was correct. Recheck it after
  the upgrade; `AWS_S3_ALLOW_UNSAFE_RENAME` in `etl/config.py` is worth reviewing.

## Roadmap

| Stage | Work | Status |
|---|---|---|
| CI foundation | Lint, tests, builds | Done, PR #2 merged |
| 1. Revenue bugs | Silver fixes, smoke test, `rustfs/rc` | Done, PR #3 merged |
| 2. Security + config | Secrets out of the repo, localhost ports, non-root, Dagster on Postgres, real checks, working CDC, Kafka in `cdc` profile, SQLMesh removed | Pushed to `fix/security-and-config`, tested locally, **PR not opened yet** |
| 3. Upgrades | `uv` + lockfile, Dagster 1.13, Ibis 12, DuckDB 2.0, deltalake 1.6, pyarrow 26, ClickHouse 26.3, pinned images, Dependabot | **In progress**, branch `chore/upgrade-and-lockfile`, **untested** |
| 4. Architecture | Native ClickHouse marts swapped in with `EXCHANGE TABLES`, one Dagster asset per table calling Python directly, Sling installed in the image as the only ingestion path | Not started |
| 5. Operations | Backups (`pg_dump` + lake bucket), Dagster failure alerts, Metabase on Postgres, memory limits | Not started |
| 6. Deploy to WSL | Publish images to GHCR, self-hosted runner deploys on merge to `main` | Not started |
| 7. Data model | Surrogate keys, SCD Type 2 customers, full-calendar `dim_date`, refunds, incremental loads, CDC into bronze | Not started, largest stage; owner may skip it |

## Next steps

1. Open the stage 2 PR, confirm CI passes, and merge it.
2. On `chore/upgrade-and-lockfile`, merge `main`, then build and test. The upgrade has
   never been built: the first attempt was stopped when the old laptop ran out of memory.
3. Fix what the major upgrades break. Likely places:
   - deltalake 1.x API: `DeltaTable.files()` in `etl/tests/test_ingest_modes.py`, plus
     `write_deltalake`, `merge` and `vacuum` in `etl/conn.py` and `etl/ingest.py`.
   - Ibis 12 + DuckDB 2.0: `etl/transform.py`, `raw_sql` usage in `etl/conn.py`, and the
     delta and postgres DuckDB extensions, which must exist for DuckDB 2.0.
   - Dagster 1.13: the `dagster.core.*` module paths in `orchestration/dagster.yaml`.
   - ClickHouse 26.3: the `DeltaLake` engine and named collection. Check whether vacuuming
     with 0 retention (`etl/conn.py`) is still needed.
   - Tags not confirmed to pull: `slingdata/sling:1.6.5`, `henrygd/beszel:0.21.1`,
     `henrygd/beszel-agent:0.21.1`.
   - The `uv lock --upgrade-package` command in the README has not been run.
4. `pip-audit` is now blocking in CI. Confirm the upgrades clear the known advisories.
   Trivy stays report-only until its first results have been reviewed.
5. Continue with stages 4 to 7.

Stage 5's Metabase move needs a one-time migration of existing dashboards from the H2
file: document it and don't delete the old data. Stage 6 needs the owner to install the
runner with a token from GitHub settings. The repo is public, so the runner must only
serve `main` and manual runs, never pull requests, and outside contributors' workflows
must require approval.

## Upgrading an existing install

See "Upgrading an existing install" in `README.md` when moving a running stack onto the
stage 2 changes: new `.env` settings, and old containers must be stopped first.
