# Medallion ETL runner: ingest -> bronze -> silver/gold/mart -> serve.
# Built from etl/ (see docker-compose.yml service: etl).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt requirements-dev.txt ./
# Tests ship in the image so the pipeline can be verified in-place:
#   docker compose run --rm etl python -m pytest tests/ -q
RUN pip install --no-cache-dir --timeout 120 --retries 10 -r requirements-dev.txt

COPY . .

# No default command: the pipeline is invoked explicitly, e.g.
#   docker compose run --rm etl python etl.py --ingest custom
CMD ["python", "etl.py", "--help"]
