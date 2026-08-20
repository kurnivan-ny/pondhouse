-- ============================================================
-- 00_init.sql — schemas + source (OLTP) tables + seed data
-- Idempotent: safe to re-run.
-- ============================================================

CREATE SCHEMA IF NOT EXISTS oltp;

-- ---------------- Source tables (oltp) ----------------
CREATE TABLE IF NOT EXISTS oltp.customers (
  customer_id SERIAL PRIMARY KEY,
  full_name   TEXT NOT NULL,
  email       TEXT NOT NULL UNIQUE,
  country     TEXT NOT NULL,
  signup_date DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS oltp.products (
  product_id   SERIAL PRIMARY KEY,
  product_name TEXT NOT NULL,
  category     TEXT NOT NULL,
  unit_price   NUMERIC(10,2) NOT NULL CHECK (unit_price >= 0)
);

CREATE TABLE IF NOT EXISTS oltp.orders (
  order_id    SERIAL PRIMARY KEY,
  customer_id INT  NOT NULL REFERENCES oltp.customers(customer_id),
  order_date  DATE NOT NULL,
  status      TEXT NOT NULL DEFAULT 'placed'
);

CREATE TABLE IF NOT EXISTS oltp.order_items (
  order_item_id SERIAL PRIMARY KEY,
  order_id      INT NOT NULL REFERENCES oltp.orders(order_id),
  product_id    INT NOT NULL REFERENCES oltp.products(product_id),
  quantity      INT NOT NULL CHECK (quantity > 0),
  unit_price    NUMERIC(10,2) NOT NULL
);

-- ---------------- Seed sample data (once) ----------------
INSERT INTO oltp.customers (customer_id, full_name, email, country, signup_date)
SELECT customer_id, full_name, email, country, signup_date::date
FROM (VALUES
  (1, 'Alice Tan',  'alice@example.com', 'SG', '2026-01-15'),
  (2, 'Bob Lim',    'bob@example.com',   'MY', '2026-02-01'),
  (3, 'Carol Ng',   'carol@example.com', 'SG', '2026-02-20'),
  (4, 'Dan Wong',   'dan@example.com',   'ID', '2026-03-05')
) AS v(customer_id, full_name, email, country, signup_date)
WHERE NOT EXISTS (SELECT 1 FROM oltp.customers);

INSERT INTO oltp.products (product_id, product_name, category, unit_price)
SELECT * FROM (VALUES
  (1, 'Laptop 14"',     'electronics', 1200.00),
  (2, 'USB-C Cable',    'accessories',   12.50),
  (3, 'Wireless Mouse', 'accessories',   25.00),
  (4, 'Monitor 27"',    'electronics',  350.00),
  (5, 'Desk Lamp',      'home',          40.00),
  (6, 'Notebook',       'stationery',     5.50)
) AS v(product_id, product_name, category, unit_price)
WHERE NOT EXISTS (SELECT 1 FROM oltp.products);

INSERT INTO oltp.orders (order_id, customer_id, order_date, status)
SELECT order_id, customer_id, order_date::date, status
FROM (VALUES
  (1, 1, '2026-08-01', 'placed'),
  (2, 2, '2026-08-02', 'shipped'),
  (3, 1, '2026-08-03', 'delivered')
) AS v(order_id, customer_id, order_date, status)
WHERE NOT EXISTS (SELECT 1 FROM oltp.orders);

INSERT INTO oltp.order_items (order_id, product_id, quantity, unit_price)
SELECT * FROM (VALUES
  (1, 1, 1, 1200.00),
  (1, 2, 2,   12.50),
  (2, 3, 1,   25.00),
  (2, 4, 1,  350.00),
  (3, 6, 3,    5.50),
  (3, 5, 1,   40.00)
) AS v(order_id, product_id, quantity, unit_price)
WHERE NOT EXISTS (SELECT 1 FROM oltp.order_items);

-- Reset sequences after seeding explicit ids so future inserts don't collide.
SELECT setval(pg_get_serial_sequence('oltp.customers', 'customer_id'),
              COALESCE((SELECT MAX(customer_id) FROM oltp.customers), 1));
SELECT setval(pg_get_serial_sequence('oltp.products', 'product_id'),
              COALESCE((SELECT MAX(product_id) FROM oltp.products), 1));
SELECT setval(pg_get_serial_sequence('oltp.orders', 'order_id'),
              COALESCE((SELECT MAX(order_id) FROM oltp.orders), 1));
SELECT setval(pg_get_serial_sequence('oltp.order_items', 'order_item_id'),
              COALESCE((SELECT MAX(order_item_id) FROM oltp.order_items), 1));
