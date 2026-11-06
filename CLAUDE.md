# pondhouse

Open-source lakehouse in Docker Compose: Postgres (POS source) -> Sling
ingestion -> Delta medallion on RustFS (bronze/silver/gold/mart, computed with Ibis on
DuckDB) -> native ClickHouse tables -> Metabase, orchestrated by Dagster. A separate CDC path runs
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
bash scripts/smoke_test.sh           # pipeline, re-run, checks, Dagster, new row, lake outage
bash scripts/smoke_cdc.sh            # row in Postgres -> readable parquet on RustFS
bash scripts/smoke_ops.sh            # Metabase H2 migration, backup + restore, alerts
```

The seed data must give exactly **6 fact rows and 1656.50 revenue** at every stage of
`smoke_test.sh`, until its later stages add a row (7 rows, 1756.50). Smoke tests fit in 8 GB
of total RAM: run alone on a 7.4 GB WSL laptop, `smoke_test.sh` kept about 4.2 GB available.
Other stacks running alongside can use that up, so check `free -m` first; `smoke_ops.sh` is
the heaviest, because Metabase takes about 1.4 GB.
CI (`.github/workflows/ci.yml`) runs lint, tests, all three smoke tests, image builds and scans.

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
- The PyPI `sling` package fetches its binary, and Sling fetches DuckDB, on first use. The
  Dockerfiles bake both with a local-file run as the runtime user, so runs need no internet
  (tested on an internal Docker network). The bake needs no connections.
- Dagster uses `in_process_executor`: a subprocess per step would start Python and import
  Ibis about 60 times on a small pipeline.
- `TRANSFORMS` in `etl/transform.py` declares each table's reads, and Dagster orders assets
  from it. A unit test fails if a declaration and the code's real reads differ.
- ClickHouse's named collection needs `named_collection_control` for the user, and 26.3
  also needs `show_named_collections_secrets`, which lets that user read the S3 credentials
  in the collection (`serving/users.d/named_collections.xml`). The user must match
  `CLICKHOUSE_USER`.
- ClickHouse rejects a Delta table whose files have different schemas, so `etl/bronze.py`
  casts Sling's wider types back to one schema (`SOURCE_SCHEMAS`).
- ClickHouse 26.3 follows the Delta log: with vacuuming off, tombstoned files (including an
  older schema) did not change results in an overwrite-mode test. `vacuum_delta` stays
  only to stop files piling up, so a longer retention is now an option.
- Aiven sink: `file.name.prefix` is ignored when a template is set, so `cdc/` is in
  `file.name.template`. The legacy `aws.s3.prefix` setting overrides the template.
- Trivy blocks CI for `etl` and `dagster` (0 findings) but only reports for `connect`,
  which has 76 HIGH/CRITICAL findings, 47 of them inside the Aiven sink, even on the latest
  Debezium and Aiven releases. Connect listens on localhost only. Recheck on upgrades.
- Trivy also ignores the Sling binary when blocking, while still reporting it: it has 42
  HIGH/CRITICAL findings in compiled-in Go libraries on the latest release. Sling runs
  only for ingestion and opens no ports. Recheck on upgrades.
- Metabase v0.65's image bundles the ClickHouse driver (seen in its startup log), so it needs
  no plugin. Connecting it to the marts through the UI has not been tried.
- Metabase's `MB_DB_FILE=/metabase-data/metabase.db` creates a folder, so the H2 path is
  `/metabase-data/metabase.db/metabase.db`. The image's entrypoint also drops the argument
  of `load-from-h2`, so `scripts/migrate_metabase_h2.sh` runs `java` directly as uid 2000.
- Sling keeps a table's old parquet when the source table becomes empty, unless
  `SLING_ALLOW_EMPTY` is set (it is, in the replication file). With no matching source
  tables at all, it exits 0 having done nothing.
- Dagster run sensors only watch their own code location unless told otherwise, and a run
  from `dagster asset materialize -m` counts as a different one. Hence
  `monitor_all_code_locations=True`.
- Dependabot raises the pins' upper limits for `sqlalchemy` and `sqlglot`, which brings the
  breakage back. `.github/dependabot.yml` ignores those versions; remove the rule with the pin.
- JVM services need a heap cap below `mem_limit`. Connect used 2.1 GB by default.
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
| 2. Security + config | Secrets out of the repo, localhost ports, non-root, Dagster on Postgres, real checks, working CDC, Kafka in `cdc` profile, SQLMesh removed | Done, merged with stage 3 in PR #4 |
| 3. Upgrades | `uv` + lockfile, Dagster 1.13, Ibis 12, DuckDB 2.0, deltalake 1.6, pyarrow 26, ClickHouse 26.3, pinned images, Dependabot | Done, PR #4 merged |
| 4. Architecture | Native ClickHouse marts swapped in with `EXCHANGE TABLES`, one Dagster asset per table calling Python directly, Sling installed in the image as the only ingestion path | Done, PR #11 merged |
| 5. Operations | Backups (`pg_dump` + lake bucket), Dagster failure alerts, Metabase on Postgres, memory limits | Branch `feat/stage-5-operations`, tested locally, **PR not opened yet** |
| 6. Deploy to WSL | Publish images to GHCR, self-hosted runner deploys on merge to `main` | Not started |
| 7. Data model | Surrogate keys, SCD Type 2 customers, full-calendar `dim_date`, refunds, incremental loads, CDC into bronze | Not started, largest stage; owner may skip it |

## Next steps

1. Open the stage 5 PR, confirm CI passes, and merge it.
2. Nothing schedules `scripts/backup.sh` yet, and backups stay on the same disk by default.
   Stage 6 is a natural place to fix both.
3. Continue with stages 6 and 7.

Stage 6 needs the owner to install the
runner with a token from GitHub settings. The repo is public, so the runner must only
serve `main` and manual runs, never pull requests, and outside contributors' workflows
must require approval.

## Upgrading an existing install

See "Upgrading an existing install" in `README.md` when moving a running stack onto the
stage 2, 4 and 5 changes: new `.env` settings, old containers must be stopped first, and
Metabase's dashboards need a one-time migration from H2.
