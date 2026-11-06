# Medallion ETL runner: ingest -> bronze -> silver/gold/mart -> serve.
# Built from the repository root (it needs pyproject.toml + uv.lock). `runtime` is the
# pipeline alone; `dev` adds the tests and is what the compose `etl` service uses:
#   docker compose --profile tools run --rm etl python -m pytest tests/ -q
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    SLING_HOME=/opt/sling \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.13.7 \
    && uv sync --frozen --no-dev --no-install-project --no-cache

COPY etl/*.py ./
COPY etl/sql ./sql

COPY ingestion/sling/env.yaml "$SLING_HOME"/env.yaml
COPY ingestion/sling/replications "$SLING_HOME"/replications

RUN useradd --create-home --uid 1000 etl \
    && chown -R etl:etl "$SLING_HOME"
USER etl

# Bake everything normally fetched on first use, so runs do not depend on the network:
# the Sling binary and its DuckDB (a parquet run fetches both) and the DuckDB extensions.
RUN printf 'a\n1\n' > /tmp/bake.csv \
    && sling run --home-dir "$SLING_HOME" --src-stream file:///tmp/bake.csv --tgt-object file:///tmp/bake.parquet \
    && rm /tmp/bake.csv /tmp/bake.parquet \
    && python -c "import duckdb; duckdb.sql('INSTALL delta; INSTALL httpfs')"

# No default command: the pipeline is invoked explicitly, e.g.
#   docker compose run --rm etl python etl.py --ingest custom
CMD ["python", "etl.py", "--help"]


FROM runtime AS dev

USER root
RUN uv sync --frozen --no-install-project --no-cache
COPY etl/tests ./tests
USER etl
