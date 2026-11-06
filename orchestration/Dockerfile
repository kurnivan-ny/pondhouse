# Built from the repository root so that both orchestration/ and etl/ are in
# the build context (see docker-compose.yml: context: ., dockerfile: orchestration/Dockerfile).
FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PONDHOUSE_ETL_DIR=/opt/dagster/etl \
    PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /opt/dagster

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.13.7 \
    && uv sync --frozen --no-dev --no-install-project --no-cache --extra orchestration

# The ETL modules are top-level scripts, run by path with /opt/dagster/etl as the
# working directory.
COPY etl/*.py ./etl/
COPY etl/sql ./etl/sql
# serve.py reads ../serving/delta_marts.sql relative to itself, i.e.
# /opt/dagster/serving/delta_marts.sql.
COPY serving ./serving

COPY orchestration/workspace.yaml ./
COPY orchestration/repo ./repo

COPY orchestration/dagster.yaml "$DAGSTER_HOME"/dagster.yaml
RUN useradd --create-home --uid 1000 dagster \
    && mkdir -p "$DAGSTER_HOME/storage" \
    && chown -R dagster:dagster "$DAGSTER_HOME"
USER dagster

# Bake the DuckDB extensions so runs do not depend on the network.
RUN python -c "import duckdb; duckdb.sql('INSTALL delta; INSTALL postgres')"
