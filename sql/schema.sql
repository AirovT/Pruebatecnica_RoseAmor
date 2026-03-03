-- ============================================================
-- RoseAmor — Schema DDL
-- Autor: Jairo Torres
-- Descripción: Esquema relacional con constraints profesionales
-- ============================================================

-- Tabla de Clientes
CREATE TABLE IF NOT EXISTS customers (
    customer_id TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT NOT NULL DEFAULT 'Unknown',
    segment     TEXT NOT NULL DEFAULT 'Unknown',
    created_at  TEXT NOT NULL
);

-- Tabla de Productos
CREATE TABLE IF NOT EXISTS products (
    sku      TEXT PRIMARY KEY,
    category TEXT NOT NULL DEFAULT 'Uncategorized',
    cost     REAL NOT NULL CHECK (cost >= 0),
    active   TEXT NOT NULL DEFAULT 'True'
);

-- Tabla de Órdenes (fact table)
CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    sku         TEXT NOT NULL,
    quantity    INTEGER NOT NULL CHECK (quantity > 0),
    unit_price  REAL NOT NULL CHECK (unit_price > 0),
    order_date  TEXT NOT NULL,
    channel     TEXT NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (sku) REFERENCES products(sku)
);

-- Índices para mejorar rendimiento de queries
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_sku ON orders(sku);
CREATE INDEX IF NOT EXISTS idx_orders_channel ON orders(channel);
