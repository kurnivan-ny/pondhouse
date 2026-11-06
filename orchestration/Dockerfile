# Built from the repository root so that both orchestration/ and etl/ are in
# the build context (see docker-compose.yml: context: ., dockerfile: orchestration/Dockerfile).
FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PONDHOUSE_ETL_DIR=/opt/dagster/etl \
    PYTHONUNBUFFERED=1 \
    SLING_HOME=/opt/sling \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /opt/dagster

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.13.7 \
    && uv sync --frozen --no-dev --no-install-project --no-cache --extra orchestration

# The assets import these top-level modules; see PONDHOUSE_ETL_DIR.
COPY etl/*.py ./etl/
COPY etl/sql ./etl/sql

COPY ingestion/sling/env.yaml "$SLING_HOME"/env.yaml
COPY ingestion/sling/replications "$SLING_HOME"/replications

COPY orchestration/workspace.yaml ./
COPY orchestration/repo ./repo

COPY orchestration/dagster.yaml "$DAGSTER_HOME"/dagster.yaml
RUN useradd --create-home --uid 1000 dagster \
    && mkdir -p "$DAGSTER_HOME/storage" \
    && chown -R dagster:dagster "$DAGSTER_HOME" "$SLING_HOME"
USER dagster

# Bake everything normally fetched on first use, so runs do not depend on the network:
# the Sling binary and its DuckDB (a parquet run fetches both) and the DuckDB extensions.
RUN printf 'a\n1\n' > /tmp/bake.csv \
    && sling run --home-dir "$SLING_HOME" --src-stream file:///tmp/bake.csv --tgt-object file:///tmp/bake.parquet \
    && rm /tmp/bake.csv /tmp/bake.parquet \
    && python -c "import duckdb; duckdb.sql('INSTALL delta; INSTALL httpfs')"
