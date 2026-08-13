FROM python:3.12-slim

ENV DAGSTER_HOME=/opt/dagster
WORKDIR /opt/dagster

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dagster.yaml workspace.yaml ./
COPY repo ./repo
RUN mkdir -p /opt/dagster/dagster_home
