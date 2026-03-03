-- ============================================================
-- RoseAmor — KPIs & Queries para Reportes
-- Autor: Jairo Torres
-- Descripción: Consultas SQL para los KPIs y visualizaciones
--              solicitados en la prueba técnica.
-- Nota: Todas usan COALESCE para manejar nulos y CTEs para
--        legibilidad profesional.
-- ============================================================


-- ═══════════════════════════════════════════════════════════
-- TARJETAS KPI
-- ═══════════════════════════════════════════════════════════

-- KPI 1: Ventas Totales
SELECT
    ROUND(SUM(COALESCE(quantity, 0) * COALESCE(unit_price, 0)), 2) AS ventas_totales
FROM orders;


-- KPI 2: Margen Total (Utilidad)
SELECT
    ROUND(
        SUM(
            COALESCE(o.quantity, 0) * (COALESCE(o.unit_price, 0) - COALESCE(p.cost, 0))
        ), 2
    ) AS margen_total
FROM orders o
LEFT JOIN products p ON o.sku = p.sku;


-- KPI 3: Número de Pedidos Únicos
SELECT
    COUNT(DISTINCT order_id) AS numero_pedidos
FROM orders;


-- KPI 4: Ticket Promedio (Ventas / Pedidos)
WITH metricas AS (
    SELECT
        ROUND(SUM(COALESCE(quantity, 0) * COALESCE(unit_price, 0)), 2) AS ventas_totales,
        COUNT(DISTINCT order_id) AS numero_pedidos
    FROM orders
)
SELECT
    ventas_totales,
    numero_pedidos,
    ROUND(ventas_totales / NULLIF(numero_pedidos, 0), 2) AS ticket_promedio
FROM metricas;


-- ═══════════════════════════════════════════════════════════
-- VISUALIZACIONES
-- ═══════════════════════════════════════════════════════════

-- VIZ 1: Ventas por Mes
SELECT
    strftime('%Y-%m', order_date) AS mes,
    ROUND(SUM(COALESCE(quantity, 0) * COALESCE(unit_price, 0)), 2) AS ventas,
    COUNT(DISTINCT order_id) AS pedidos
FROM orders
GROUP BY strftime('%Y-%m', order_date)
ORDER BY mes;


-- VIZ 2: Ventas por Canal
SELECT
    channel AS canal,
    ROUND(SUM(COALESCE(quantity, 0) * COALESCE(unit_price, 0)), 2) AS ventas,
    COUNT(DISTINCT order_id) AS pedidos,
    ROUND(AVG(COALESCE(quantity, 0) * COALESCE(unit_price, 0)), 2) AS ticket_promedio
FROM orders
GROUP BY channel
ORDER BY ventas DESC;


-- VIZ 3: Margen por Categoría
SELECT
    COALESCE(p.category, 'Sin Categoría') AS categoria,
    ROUND(SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0)), 2) AS ventas,
    ROUND(
        SUM(
            COALESCE(o.quantity, 0) * (COALESCE(o.unit_price, 0) - COALESCE(p.cost, 0))
        ), 2
    ) AS margen,
    ROUND(
        CASE
            WHEN SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0)) = 0 THEN 0
            ELSE SUM(COALESCE(o.quantity, 0) * (COALESCE(o.unit_price, 0) - COALESCE(p.cost, 0)))
                 * 100.0
                 / SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0))
        END, 1
    ) AS pct_margen
FROM orders o
LEFT JOIN products p ON o.sku = p.sku
GROUP BY p.category
ORDER BY margen DESC;


-- VIZ 4: Top 10 Clientes por Ingresos
SELECT
    c.customer_id,
    c.name AS cliente,
    c.country AS pais,
    c.segment AS segmento,
    ROUND(SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0)), 2) AS ingresos,
    COUNT(DISTINCT o.order_id) AS pedidos,
    ROUND(
        SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0))
        / NULLIF(COUNT(DISTINCT o.order_id), 0), 2
    ) AS ticket_promedio
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name, c.country, c.segment
ORDER BY ingresos DESC
LIMIT 10;


-- VIZ 5: Top 10 Productos Más Vendidos (por ingresos)
SELECT
    p.sku,
    COALESCE(p.category, 'Sin Categoría') AS categoria,
    SUM(COALESCE(o.quantity, 0)) AS unidades_vendidas,
    ROUND(SUM(COALESCE(o.quantity, 0) * COALESCE(o.unit_price, 0)), 2) AS ingresos,
    ROUND(
        SUM(
            COALESCE(o.quantity, 0) * (COALESCE(o.unit_price, 0) - COALESCE(p.cost, 0))
        ), 2
    ) AS margen,
    p.cost AS costo_unitario
FROM orders o
JOIN products p ON o.sku = p.sku
GROUP BY p.sku, p.category, p.cost
ORDER BY ingresos DESC
LIMIT 10;


-- ═══════════════════════════════════════════════════════════
-- FILTROS / DIMENSIONES DISPONIBLES
-- ═══════════════════════════════════════════════════════════

-- Lista de canales
SELECT DISTINCT channel AS canal FROM orders ORDER BY channel;

-- Lista de categorías
SELECT DISTINCT category AS categoria FROM products ORDER BY category;

-- Lista de países
SELECT DISTINCT country AS pais FROM customers ORDER BY country;

-- Rango de fechas disponibles
SELECT
    MIN(order_date) AS fecha_inicio,
    MAX(order_date) AS fecha_fin
FROM orders;
