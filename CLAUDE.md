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
`smoke_test.sh`. Smoke tests are the heaviest step, but they fit in 8 GB of total RAM: on
a 7.4 GB WSL laptop, available memory never fell below about 4.3 GB, with under 25 MB of swap.
CI (`.github/workflows/ci.yml`) runs lint, tests, both smoke tests, image builds and scans.

## Gotchas already learned

- On Windows + WSL, run git from inside WSL. Windows git with `autocrlf=true` rewrites
  working files to CRLF. `.gitattributes` on the stage 3 branch guards against this.
- `docker manifest inspect` gave false negatives here. Test image availability with a
  real `docker pull`. Pulls also failed at times from IPv6 "network is unreachable";
  retry before concluding an image is gone.
- `minio/mc` can no longer be pulled, so buckets are created with `rustfs/rc`.
- `slingdata/sling` version tags need a `v` prefix: `v1.6.5` exists, `1.6.5` does not.
- SQLAlchemy 2.1 changes the default Postgres driver to psycopg v3, which broke the
  NOTIFY statements in dagster-postgres 0.23. With dagster-postgres 0.29 and 2.1.8, Dagster
  fails on startup with `No module named 'psycopg'`, so `pyproject.toml` pins `<2.1`.
- sqlglot 30.18 and later read `Drop`'s table from `tables`, but Ibis 12.0.0 passes `this`,
  so Ibis renders `DROP VIEW IF EXISTS` with no name and `create_table` with in-memory data
  fails. `pyproject.toml` pins `sqlglot<30.18`. The pipeline doesn't call `create_table`, so
  smoke tests pass without the pin and only the unit tests catch this. Drop the pin once
  a newer Ibis handles it.
- `dagster.yaml` must live in `$DAGSTER_HOME`; otherwise Dagster silently uses SQLite.
- ClickHouse's named collection needs `named_collection_control` for the user, and 26.3
  also needs `show_named_collections_secrets`, which lets that user read the S3 credentials
  in the collection (`serving/users.d/named_collections.xml`). The user must match
  `CLICKHOUSE_USER`.
- ClickHouse rejects a Delta table whose files have different schemas, so `etl/bronze.py`
  casts to one schema (`SOURCE_SCHEMAS`) whichever ingestion path ran.
- ClickHouse 26.3 follows the Delta log: with vacuuming off, tombstoned files (including an
  older schema) did not change results in an overwrite-mode test. `vacuum_delta` stays
  only to stop files piling up, so a longer retention is now an option.
- Aiven sink: `file.name.prefix` is ignored when a template is set, so `cdc/` is in
  `file.name.template`. The legacy `aws.s3.prefix` setting overrides the template.
- GitHub runners use a different uid than 1000, hence `SLING_USER` in `smoke_test.sh`.
- Trivy blocks CI for `etl` and `dagster` (0 findings) but only reports for `connect`,
  which has 76 HIGH/CRITICAL findings, 47 of them inside the Aiven sink, even on the latest
  Debezium and Aiven releases. Connect listens on localhost only. Recheck on upgrades.
- Unverified: whether Metabase needs a separate ClickHouse driver plugin.
- Not run yet: the `governance` (OpenMetadata) and `monitoring` (Beszel) profiles. Only
  their compose config has been validated.
- Before the upgrade, a `deltalake` warning appeared on many writes ("Attempting to write
  a transaction N but the underlying table has been updated to N"). It did not appear in
  the upgraded smoke tests. `AWS_S3_ALLOW_UNSAFE_RENAME` in `etl/config.py` is still worth
  reviewing.

## Roadmap

| Stage | Work | Status |
|---|---|---|
| CI foundation | Lint, tests, builds | Done, PR #2 merged |
| 1. Revenue bugs | Silver fixes, smoke test, `rustfs/rc` | Done, PR #3 merged |
| 2. Security + config | Secrets out of the repo, localhost ports, non-root, Dagster on Postgres, real checks, working CDC, Kafka in `cdc` profile, SQLMesh removed | Pushed to `fix/security-and-config`, tested locally, **PR not opened yet** |
| 3. Upgrades | `uv` + lockfile, Dagster 1.13, Ibis 12, DuckDB 2.0, deltalake 1.6, pyarrow 26, ClickHouse 26.3, pinned images, Dependabot | **In progress**, branch `chore/upgrade-and-lockfile`. Unit and both smoke tests pass locally; CI not run yet, **PR not opened yet** |
| 4. Architecture | Native ClickHouse marts swapped in with `EXCHANGE TABLES`, one Dagster asset per table calling Python directly, Sling installed in the image as the only ingestion path | Not started |
| 5. Operations | Backups (`pg_dump` + lake bucket), Dagster failure alerts, Metabase on Postgres, memory limits | Not started |
| 6. Deploy to WSL | Publish images to GHCR, self-hosted runner deploys on merge to `main` | Not started |
| 7. Data model | Surrogate keys, SCD Type 2 customers, full-calendar `dim_date`, refunds, incremental loads, CDC into bronze | Not started, largest stage; owner may skip it |

## Next steps

1. Open the stage 2 PR, confirm CI passes, and merge it.
2. Open the stage 3 PR from `chore/upgrade-and-lockfile` and confirm CI passes on a clean
   machine. Locally, all 71 unit tests, both smoke tests, `ruff`, `pip-audit`, Hadolint and
   Gitleaks pass.
3. Continue with stages 4 to 7, once stages 2 and 3 are merged.

Stage 5's Metabase move needs a one-time migration of existing dashboards from the H2
file: document it and don't delete the old data. Stage 6 needs the owner to install the
runner with a token from GitHub settings. The repo is public, so the runner must only
serve `main` and manual runs, never pull requests, and outside contributors' workflows
must require approval.

## Upgrading an existing install

See "Upgrading an existing install" in `README.md` when moving a running stack onto the
stage 2 changes: new `.env` settings, and old containers must be stopped first.
