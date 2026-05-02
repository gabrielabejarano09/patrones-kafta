-- ================================================================
-- DB LOGISTICA: Inventory + Shipping
-- ================================================================

CREATE TABLE IF NOT EXISTS inventory (
  id                  SERIAL PRIMARY KEY,
  product_id          VARCHAR(50) NOT NULL UNIQUE,
  stock_quantity      INT NOT NULL DEFAULT 0,
  reserved_quantity   INT NOT NULL DEFAULT 0,
  updated_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS inventory_reservations (
  id          SERIAL PRIMARY KEY,
  order_id    VARCHAR(50) NOT NULL,
  product_id  VARCHAR(50) NOT NULL,
  quantity    INT NOT NULL,
  status      VARCHAR(20) DEFAULT 'RESERVED',  -- RESERVED | RELEASED | COMMITTED
  event_id    VARCHAR(100) UNIQUE,  -- idempotencia
  created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS shipments (
  id               VARCHAR(50) PRIMARY KEY,
  order_id         VARCHAR(50) NOT NULL UNIQUE,
  status           VARCHAR(30) DEFAULT 'CREATED',
  tracking_number  VARCHAR(50),
  event_id         VARCHAR(100) UNIQUE,  -- idempotencia
  created_at       TIMESTAMP DEFAULT NOW()
);

-- ── Stock inicial de productos ────────────────────────────────────────
INSERT INTO inventory (product_id, stock_quantity, reserved_quantity) VALUES
  ('prod_001', 10, 0),
  ('prod_002', 50, 0),
  ('prod_003', 30, 0),
  ('prod_004', 20, 0),
  ('prod_005', 15, 0)
ON CONFLICT DO NOTHING;
