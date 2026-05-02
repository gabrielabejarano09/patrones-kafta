-- ── Tabla de clientes ────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS customers (
  id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) UNIQUE NOT NULL,
  phone VARCHAR(50),
  address TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 
-- ── Tabla de productos ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS products (
  id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  price DECIMAL(12,2) NOT NULL,
  description TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 
-- ── Tabla de órdenes ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS orders (
  id VARCHAR(100) PRIMARY KEY,
  customer_id VARCHAR(50) REFERENCES customers(id),
  status VARCHAR(50) DEFAULT 'PENDING',
  total_amount DECIMAL(12,2),
  event_id VARCHAR(100) UNIQUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 
-- ── Tabla de items de orden ──────────────────────────────────────────
CREATE TABLE IF NOT EXISTS order_items (
  id SERIAL PRIMARY KEY,
  order_id VARCHAR(100) REFERENCES orders(id),
  product_id VARCHAR(50),
  product_name VARCHAR(255),
  quantity INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  subtotal DECIMAL(12,2) NOT NULL
);
 
-- ── Tabla de pagos ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS payments (
  id VARCHAR(100) PRIMARY KEY,
  order_id VARCHAR(100) REFERENCES orders(id),
  amount DECIMAL(12,2) NOT NULL,
  status VARCHAR(50) DEFAULT 'PROCESSED',
  event_id VARCHAR(100) UNIQUE,
  processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
 
-- ── Datos iniciales: Clientes ─────────────────────────────────────────
INSERT INTO customers (id, name, email, phone, address) VALUES
  ('cust_001', 'Daniel Saavedra', 'danielsafo@unisabana.edu.co',
   '+57 310 000 0001', 'Bogotá, Colombia'),
  ('cust_002', 'Daniel Saavedra Gmail', 'daniel.saavedra.fon@gmail.com',
   '+57 310 000 0002', 'Bogotá, Colombia'),
  ('cust_003', 'Test User', 'testuser@example.com',
   '+57 310 000 0003', 'Medellín, Colombia')
ON CONFLICT (id) DO NOTHING;
 
-- ── Datos iniciales: Productos ────────────────────────────────────────
INSERT INTO products (id, name, price, description) VALUES
  ('prod_001', 'Laptop Pro 15"', 3500000, 'Laptop de alto rendimiento 15 pulgadas'),
  ('prod_002', 'Mouse Inalambrico', 65000, 'Mouse ergonómico inalámbrico'),
  ('prod_003', 'Hub USB-C 7 en 1', 120000, 'Hub multipuerto USB-C'),
  ('prod_004', 'Teclado Mecanico', 230000, 'Teclado mecánico RGB'),
  ('prod_005', 'Monitor 24" FHD', 850000, 'Monitor Full HD 24 pulgadas')
ON CONFLICT (id) DO NOTHING;
