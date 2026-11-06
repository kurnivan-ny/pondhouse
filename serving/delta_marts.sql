-- ClickHouse serving tables: read the Delta gold + mart layers from RustFS.
-- Applied by etl/serve.py after the transform step (once Delta data exists).
-- Credentials come from the `rustfs` named collection (config.d/rustfs_s3.xml).

CREATE DATABASE IF NOT EXISTS marts;

-- Gold star-schema dims + fact
CREATE OR REPLACE TABLE marts.dim_customer
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/gold/dim_customer/');

CREATE OR REPLACE TABLE marts.dim_product
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/gold/dim_product/');

CREATE OR REPLACE TABLE marts.dim_store
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/gold/dim_store/');

CREATE OR REPLACE TABLE marts.dim_date
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/gold/dim_date/');

CREATE OR REPLACE TABLE marts.fact_sales
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/gold/fact_sales/');

-- Data marts
CREATE OR REPLACE TABLE marts.daily_sales_mart
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/mart/daily_sales_mart/');

CREATE OR REPLACE TABLE marts.product_sales_mart
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/mart/product_sales_mart/');

CREATE OR REPLACE TABLE marts.customer_sales_mart
ENGINE = DeltaLake(rustfs, url = 'http://rustfs:9000/lake/mart/customer_sales_mart/');
