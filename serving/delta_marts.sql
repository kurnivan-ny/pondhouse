-- ClickHouse serving tables: read the Delta gold + mart layers from RustFS.
-- Applied by etl/serve.py after the transform step (once Delta data exists).

CREATE DATABASE IF NOT EXISTS marts;

-- Gold star-schema dims + fact
CREATE OR REPLACE TABLE marts.dim_customer
ENGINE = DeltaLake('http://rustfs:9000/lake/gold/dim_customer/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.dim_product
ENGINE = DeltaLake('http://rustfs:9000/lake/gold/dim_product/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.dim_store
ENGINE = DeltaLake('http://rustfs:9000/lake/gold/dim_store/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.dim_date
ENGINE = DeltaLake('http://rustfs:9000/lake/gold/dim_date/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.fact_sales
ENGINE = DeltaLake('http://rustfs:9000/lake/gold/fact_sales/', 'pondhouse', 'pondhouse-secret-change-me');

-- Data marts
CREATE OR REPLACE TABLE marts.daily_sales_mart
ENGINE = DeltaLake('http://rustfs:9000/lake/mart/daily_sales_mart/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.product_sales_mart
ENGINE = DeltaLake('http://rustfs:9000/lake/mart/product_sales_mart/', 'pondhouse', 'pondhouse-secret-change-me');

CREATE OR REPLACE TABLE marts.customer_sales_mart
ENGINE = DeltaLake('http://rustfs:9000/lake/mart/customer_sales_mart/', 'pondhouse', 'pondhouse-secret-change-me');
