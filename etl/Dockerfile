# Medallion ETL runner: ingest -> bronze -> silver/gold/mart -> serve.
# Built from the repository root (it needs pyproject.toml + uv.lock). `runtime` is the
# pipeline alone; `dev` adds the tests and is what the compose `etl` service uses:
#   docker compose --profile tools run --rm etl python -m pytest tests/ -q
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    UV_PROJECT_ENVIRONMENT=/usr/local

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.13.7 \
    && uv sync --frozen --no-dev --no-install-project --no-cache

COPY etl/*.py ./
COPY etl/sql ./sql

RUN useradd --create-home --uid 1000 etl
USER etl

# Bake the DuckDB extensions so runs do not depend on the network.
RUN python -c "import duckdb; duckdb.sql('INSTALL delta; INSTALL postgres')"

# No default command: the pipeline is invoked explicitly, e.g.
#   docker compose run --rm etl python etl.py --ingest custom
CMD ["python", "etl.py", "--help"]


FROM runtime AS dev

USER root
RUN uv sync --frozen --no-install-project --no-cache
COPY etl/tests ./tests
USER etl
