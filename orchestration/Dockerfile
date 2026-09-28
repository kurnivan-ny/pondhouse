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

# ETL modules are top-level scripts (etl.py shadows the package name), so they
# are run as `python -m <module>` with /opt/dagster/etl as the working directory.
COPY etl ./etl
# serve.py reads ../serving/delta_marts.sql relative to itself, i.e.
# /opt/dagster/serving/delta_marts.sql.
COPY serving ./serving

COPY orchestration/dagster.yaml orchestration/workspace.yaml ./
COPY orchestration/repo ./repo

RUN mkdir -p "$DAGSTER_HOME"
