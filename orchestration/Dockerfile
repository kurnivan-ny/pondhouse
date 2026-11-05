# Built from the repository root so that both orchestration/ and etl/ are in
# the build context (see docker-compose.yml: context: ., dockerfile: orchestration/Dockerfile).
FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home \
    PONDHOUSE_ETL_DIR=/opt/dagster/etl \
    PYTHONUNBUFFERED=1

WORKDIR /opt/dagster

COPY orchestration/requirements.txt .
# Dagster pulls a large dependency tree; the default 15s socket timeout is not
# enough on slower links.
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt

# The ETL modules are top-level scripts, run by path with /opt/dagster/etl as the
# working directory.
COPY etl ./etl
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
