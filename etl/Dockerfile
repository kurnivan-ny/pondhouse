# Medallion ETL runner: ingest -> bronze -> silver/gold/mart -> serve.
# Built from etl/. `runtime` is the pipeline alone; `dev` adds the tests and is what
# the compose `etl` service uses, so they can be run in place:
#   docker compose --profile tools run --rm etl python -m pytest tests/ -q
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements.txt

COPY *.py ./
COPY sql ./sql

RUN useradd --create-home --uid 1000 etl
USER etl

# Bake the DuckDB extensions so runs do not depend on the network.
RUN python -c "import duckdb; duckdb.sql('INSTALL delta; INSTALL postgres')"

# No default command: the pipeline is invoked explicitly, e.g.
#   docker compose run --rm etl python etl.py --ingest custom
CMD ["python", "etl.py", "--help"]


FROM runtime AS dev

USER root
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements-dev.txt
COPY tests ./tests
USER etl
