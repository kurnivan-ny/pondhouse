-- Runs once on first postgres boot (mounted to /docker-entrypoint-initdb.d)
-- NOTE: Passwords should be synchronized with .env and docker-compose.yml
-- For production, use a generated init script or secrets management.
CREATE DATABASE dagster;          -- Dagster run/event/schedule storage
CREATE DATABASE demo;             -- playground source DB for Debezium CDC

-- OpenMetadata (governance profile) — users + databases it expects
-- Default passwords match docker-compose.yml environment variables
CREATE USER openmetadata_user WITH PASSWORD 'openmetadata_password';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user;
