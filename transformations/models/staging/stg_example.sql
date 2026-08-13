-- Example bronze model: raw CDC parquet landed on RustFS by the Aiven S3 sink.
-- Requires the DuckDB S3 secret shown in ../config.yaml.
MODEL (
  name staging.example_cdc_landing,
  kind FULL,
  description 'Raw CDC events from Kafka -> RustFS parquet landing zone'
);

SELECT
  *
FROM read_parquet('s3://cdc-landing/**/*.parquet');
