-- ============================================================
-- 00_init.sql — retail POS source schema + reference data + seed
-- Simulates a POS / SAP HANA source system (codes need translation).
-- Idempotent: safe to re-run.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS pos;

-- ---------------- Reference / lookup tables ----------------
CREATE TABLE IF NOT EXISTS pos.category_ref (
  category_code TEXT PRIMARY KEY,
  category_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pos.status_ref (
  status_code TEXT PRIMARY KEY,
  status_name  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pos.country_ref (
  country_code TEXT PRIMARY KEY,
  country_name TEXT NOT NULL
);

-- ---------------- POS tables ----------------
CREATE TABLE IF NOT EXISTS pos.stores (
  store_id     SERIAL PRIMARY KEY,
  store_name   TEXT NOT NULL,
  city         TEXT,
  country_code TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pos.products (
  product_id    SERIAL PRIMARY KEY,
  product_name  TEXT NOT NULL,
  category_code TEXT NOT NULL,
  unit_price    NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE IF NOT EXISTS pos.customers (
  customer_id  SERIAL PRIMARY KEY,
  full_name    TEXT NOT NULL,
  email        TEXT NOT NULL UNIQUE,
  country_code TEXT NOT NULL,
  signup_date  DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS pos.transactions (
  transaction_id   SERIAL PRIMARY KEY,
  store_id         INT  NOT NULL REFERENCES pos.stores(store_id),
  customer_id      INT  NOT NULL REFERENCES pos.customers(customer_id),
  transaction_date DATE NOT NULL,
  status_code      TEXT NOT NULL DEFAULT 'C',
  payment_method   TEXT NOT NULL DEFAULT 'CASH'
);

CREATE TABLE IF NOT EXISTS pos.transaction_items (
  item_id        SERIAL PRIMARY KEY,
  transaction_id INT NOT NULL REFERENCES pos.transactions(transaction_id),
  product_id     INT NOT NULL REFERENCES pos.products(product_id),
  quantity       INT NOT NULL CHECK (quantity > 0),
  unit_price     NUMERIC(10,2) NOT NULL
);

-- ---------------- Reference data ----------------
INSERT INTO pos.category_ref (category_code, category_name)
SELECT * FROM (VALUES
  ('ELEC',    'electronics'),
  ('ACCESS',  'accessories'),
  ('GROC',    'grocery'),
  ('STATION', 'stationery'),
  ('HOME',    'home')
) AS v(category_code, category_name)
WHERE NOT EXISTS (SELECT 1 FROM pos.category_ref);

INSERT INTO pos.status_ref (status_code, status_name)
SELECT * FROM (VALUES
  ('C', 'completed'),
  ('P', 'pending'),
  ('R', 'refunded'),
  ('X', 'cancelled')
) AS v(status_code, status_name)
WHERE NOT EXISTS (SELECT 1 FROM pos.status_ref);

INSERT INTO pos.country_ref (country_code, country_name)
SELECT * FROM (VALUES
  ('SG', 'Singapore'),
  ('MY', 'Malaysia'),
  ('ID', 'Indonesia'),
  ('TH', 'Thailand')
) AS v(country_code, country_name)
WHERE NOT EXISTS (SELECT 1 FROM pos.country_ref);

-- ---------------- Seed sample data ----------------
INSERT INTO pos.stores (store_id, store_name, city, country_code)
SELECT * FROM (VALUES
  (1, 'Flagship Store', 'Singapore', 'SG'),
  (2, 'Mall Kiosk',     'Kuala Lumpur', 'MY'),
  (3, 'Airport Outlet', 'Jakarta',   'ID')
) AS v(store_id, store_name, city, country_code)
WHERE NOT EXISTS (SELECT 1 FROM pos.stores);

INSERT INTO pos.products (product_id, product_name, category_code, unit_price)
SELECT * FROM (VALUES
  (1, 'Laptop 14"',     'ELEC',    1200.00),
  (2, 'USB-C Cable',    'ACCESS',    12.50),
  (3, 'Wireless Mouse', 'ACCESS',    25.00),
  (4, 'Monitor 27"',    'ELEC',     350.00),
  (5, 'Desk Lamp',      'HOME',      40.00),
  (6, 'Notebook',       'STATION',    5.50)
) AS v(product_id, product_name, category_code, unit_price)
WHERE NOT EXISTS (SELECT 1 FROM pos.products);

INSERT INTO pos.customers (customer_id, full_name, email, country_code, signup_date)
SELECT customer_id, full_name, email, country_code, signup_date::date
FROM (VALUES
  (1, 'Alice Tan',  'alice@example.com', 'SG', '2026-01-15'),
  (2, 'Bob Lim',    'bob@example.com',   'MY', '2026-02-01'),
  (3, 'Carol Ng',   'carol@example.com', 'SG', '2026-02-20'),
  (4, 'Dan Wong',   'dan@example.com',   'ID', '2026-03-05')
) AS v(customer_id, full_name, email, country_code, signup_date)
WHERE NOT EXISTS (SELECT 1 FROM pos.customers);

INSERT INTO pos.transactions (transaction_id, store_id, customer_id, transaction_date, status_code, payment_method)
SELECT transaction_id, store_id, customer_id, transaction_date::date, status_code, payment_method
FROM (VALUES
  (1, 1, 1, '2026-08-01', 'C', 'CARD'),
  (2, 2, 2, '2026-08-02', 'C', 'CASH'),
  (3, 1, 1, '2026-08-03', 'C', 'EWALLET')
) AS v(transaction_id, store_id, customer_id, transaction_date, status_code, payment_method)
WHERE NOT EXISTS (SELECT 1 FROM pos.transactions);

INSERT INTO pos.transaction_items (transaction_id, product_id, quantity, unit_price)
SELECT * FROM (VALUES
  (1, 1, 1, 1200.00),
  (1, 2, 2,   12.50),
  (2, 3, 1,   25.00),
  (2, 4, 1,  350.00),
  (3, 6, 3,    5.50),
  (3, 5, 1,   40.00)
) AS v(transaction_id, product_id, quantity, unit_price)
WHERE NOT EXISTS (SELECT 1 FROM pos.transaction_items);

-- Reset sequences after seeding explicit ids.
SELECT setval(pg_get_serial_sequence('pos.stores', 'store_id'),
              COALESCE((SELECT MAX(store_id) FROM pos.stores), 1));
SELECT setval(pg_get_serial_sequence('pos.products', 'product_id'),
              COALESCE((SELECT MAX(product_id) FROM pos.products), 1));
SELECT setval(pg_get_serial_sequence('pos.customers', 'customer_id'),
              COALESCE((SELECT MAX(customer_id) FROM pos.customers), 1));
SELECT setval(pg_get_serial_sequence('pos.transactions', 'transaction_id'),
              COALESCE((SELECT MAX(transaction_id) FROM pos.transactions), 1));
SELECT setval(pg_get_serial_sequence('pos.transaction_items', 'item_id'),
              COALESCE((SELECT MAX(item_id) FROM pos.transaction_items), 1));
