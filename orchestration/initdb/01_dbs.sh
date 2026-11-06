#!/bin/bash
# Runs once on first postgres boot (mounted to /docker-entrypoint-initdb.d).
# Passwords come from the environment, set from .env in docker-compose.yml.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -v om_password="$OM_DB_PASSWORD" -v airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE DATABASE dagster;          -- Dagster run/event/schedule storage
CREATE DATABASE demo;             -- playground source DB for Debezium CDC

-- OpenMetadata (governance profile): users + databases it expects
CREATE USER openmetadata_user WITH PASSWORD :'om_password';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;
CREATE USER airflow_user WITH PASSWORD :'airflow_password';
CREATE DATABASE airflow_db OWNER airflow_user;
SQL
