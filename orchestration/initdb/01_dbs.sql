-- Runs once on first postgres boot (mounted to /docker-entrypoint-initdb.d)
CREATE DATABASE dagster;          -- Dagster run/event/schedule storage
CREATE DATABASE demo;             -- playground source DB for Debezium CDC

-- OpenMetadata (governance profile) — users + databases it expects
CREATE USER openmetadata_user WITH PASSWORD 'openmetadata_password';
CREATE DATABASE openmetadata_db OWNER openmetadata_user;
CREATE USER airflow_user WITH PASSWORD 'airflow_pass';
CREATE DATABASE airflow_db OWNER airflow_user;
