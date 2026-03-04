"""
ETL Pipeline — Prueba Técnica RoseAmor
Autor: Jairo Torres
Descripción: Pipeline profesional de limpieza y carga de datos.

Buenas Prácticas Implementadas:
  - RAW ZONE inmutable: los CSVs originales NUNCA se modifican
  - Archivado automático: al procesar, los raw se copian a archive/ con timestamp
  - Processed exports: los datos limpios se exportan como CSVs en data/processed/
  - Idempotente: cada ejecución recrea la BD y los CSVs procesados desde cero
  - Logging: se genera un log detallado en data/logs/
  - Reutilizable: mañana cae un CSV nuevo → se pone en data/raw/ → se re-ejecuta
"""

import pandas as pd
import sqlite3
import os
import sys
import shutil
from datetime import datetime

# ─── Configuración ───────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
PROCESSED_DIR = os.path.join(BASE_DIR, "data", "processed")
ARCHIVE_DIR = os.path.join(BASE_DIR, "data", "archive")
LOGS_DIR = os.path.join(BASE_DIR, "data", "logs")
DB_PATH = os.path.join(BASE_DIR, "data", "roseamor.db")

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = None

# ─── Helpers ──────────────────────────────────────────────────────
def log(msg, level="INFO"):
    """Escribe en consola y en archivo de log."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"  [{timestamp}] [{level}] {msg}"
    print(line)
    if LOG_FILE:
        LOG_FILE.write(line + "\n")

def separator(title):
    line1 = f"\n{'='*60}"
    line2 = f"  {title}"
    line3 = f"{'='*60}"
    for l in [line1, line2, line3]:
        print(l)
        if LOG_FILE:
            LOG_FILE.write(l + "\n")

def ensure_dirs():
    """Crea los directorios necesarios si no existen."""
    for d in [RAW_DIR, PROCESSED_DIR, ARCHIVE_DIR, LOGS_DIR]:
        os.makedirs(d, exist_ok=True)

# ─── 0. ARCHIVADO DE ORIGINALES ──────────────────────────────────
def archive_raw_files():
    """
    Copia los CSVs originales a data/archive/ con timestamp.
    Los originales en data/raw/ NUNCA se modifican ni eliminan.
    """
    separator("0. ARCHIVADO DE ORIGINALES (Inmutabilidad)")
    
    archive_subdir = os.path.join(ARCHIVE_DIR, TIMESTAMP)
    os.makedirs(archive_subdir, exist_ok=True)
    
    archived = 0
    for filename in os.listdir(RAW_DIR):
        if filename.endswith(".csv"):
            src = os.path.join(RAW_DIR, filename)
            dst = os.path.join(archive_subdir, filename)
            shutil.copy2(src, dst)  # copy2 preserva metadata
            archived += 1
            log(f"Archivado: {filename} → archive/{TIMESTAMP}/{filename}")
    
    log(f"Total archivados: {archived} archivos")
    log(f"Directorio: {archive_subdir}")
    return archive_subdir

# ─── 1. CARGA DE DATOS CRUDOS ────────────────────────────────────
def load_raw_data():
    """Lee CSVs desde la raw zone (sin modificarlos)."""
    separator("1. CARGA DE DATOS CRUDOS (Read-Only)")
    
    customers_path = os.path.join(RAW_DIR, "customers.csv")
    products_path = os.path.join(RAW_DIR, "products.csv")
    orders_path = os.path.join(RAW_DIR, "orders.csv")
    
    # Verificar que existen
    for path, name in [(customers_path, "customers.csv"), 
                        (products_path, "products.csv"),
                        (orders_path, "orders.csv")]:
        if not os.path.exists(path):
            log(f"ERROR: No se encontró {name} en {RAW_DIR}", "ERROR")
            sys.exit(1)
    
    customers = pd.read_csv(customers_path)
    products = pd.read_csv(products_path)
    orders = pd.read_csv(orders_path)
    
    log(f"customers.csv: {len(customers)} filas, {customers.shape[1]} columnas")
    log(f"products.csv:  {len(products)} filas, {products.shape[1]} columnas")
    log(f"orders.csv:    {len(orders)} filas, {orders.shape[1]} columnas")
    
    return customers, products, orders

# ─── 2. DATA PROFILING ───────────────────────────────────────────
def profile_data(customers, products, orders):
    """Análisis de calidad sin modificar los DataFrames."""
    separator("2. DATA PROFILING — Hallazgos de Calidad")
    
    issues = {}
    
    # Customers
    issues["null_country"] = int(customers["country"].isna().sum())
    issues["null_segment"] = int(customers["segment"].isna().sum())
    log(f"Customers — países nulos: {issues['null_country']}, segmentos nulos: {issues['null_segment']}")
    
    # Products
    issues["negative_cost"] = int((products["cost"] < 0).sum())
    issues["null_category"] = int(products["category"].isna().sum())
    issues["inactive"] = int((products["active"] == False).sum())
    log(f"Products  — costos negativos: {issues['negative_cost']}, categoría nula: {issues['null_category']}, inactivos: {issues['inactive']}")
    
    # Orders
    issues["null_price"] = int(orders["unit_price"].isna().sum())
    issues["negative_qty"] = int((orders["quantity"] < 0).sum())
    issues["duplicate_ids"] = int(orders["order_id"].duplicated().sum())
    
    # Fechas inválidas — usar pd.to_datetime con errors='coerce'
    parsed = pd.to_datetime(orders["order_date"], errors="coerce")
    issues["invalid_dates"] = int(parsed.isna().sum())
    
    log(f"Orders    — unit_price nulo: {issues['null_price']}, quantity negativa: {issues['negative_qty']}")
    log(f"            duplicados: {issues['duplicate_ids']}, fechas inválidas: {issues['invalid_dates']}")
    
    return issues

# ─── 3. LIMPIEZA DE CUSTOMERS ────────────────────────────────────
def clean_customers(df):
    """Limpia customers. Retorna COPIA limpia (original intacto)."""
    separator("3. LIMPIEZA — Customers")
    original = len(df)
    
    # Trabajar con COPIA
    clean = df.copy()
    
    # Rellenar nulos
    clean["country"] = clean["country"].fillna("Unknown")
    clean["segment"] = clean["segment"].fillna("Unknown")
    
    # Parsear fecha
    clean["created_at"] = pd.to_datetime(clean["created_at"])
    
    # Eliminar duplicados
    clean = clean.drop_duplicates(subset=["customer_id"], keep="first")
    
    log(f"Filas: {original} originales → {len(clean)} limpias")
    log(f"Países 'Unknown': {(clean['country'] == 'Unknown').sum()}")
    log(f"Segmentos 'Unknown': {(clean['segment'] == 'Unknown').sum()}")
    
    return clean

# ─── 4. LIMPIEZA DE PRODUCTS ─────────────────────────────────────
def clean_products(df):
    """Limpia products. Retorna COPIA limpia."""
    separator("4. LIMPIEZA — Products")
    original = len(df)
    
    clean = df.copy()
    
    # Costos negativos → valor absoluto
    negatives = clean[clean["cost"] < 0]
    if len(negatives) > 0:
        log(f"Corrigiendo {len(negatives)} costos negativos: {negatives['sku'].tolist()}")
        clean["cost"] = clean["cost"].abs()
    
    # Categoría nula
    clean["category"] = clean["category"].fillna("Uncategorized")
    
    # Duplicados
    clean = clean.drop_duplicates(subset=["sku"], keep="first")
    
    log(f"Filas: {original} originales → {len(clean)} limpias")
    
    return clean

# ─── 5. LIMPIEZA DE ORDERS ───────────────────────────────────────
def clean_orders(df, products_df):
    """Limpia orders. Retorna COPIA limpia."""
    separator("5. LIMPIEZA — Orders")
    original = len(df)
    
    clean = df.copy()
    
    # 5a. Duplicados
    dupes = clean["order_id"].duplicated().sum()
    clean = clean.drop_duplicates(subset=["order_id"], keep="first")
    log(f"Duplicados eliminados: {dupes}")
    
    # 5b. Fechas inválidas — vectorizado (no apply)
    clean["order_date"] = pd.to_datetime(clean["order_date"], errors="coerce")
    invalid = clean["order_date"].isna().sum()
    clean = clean.dropna(subset=["order_date"])
    log(f"Fechas inválidas eliminadas: {invalid}")
    
    # 5c. Quantities negativas
    neg_qty = (clean["quantity"] < 0).sum()
    # Quantities = 0 también se filtran
    zero_qty = (clean["quantity"] == 0).sum()
    clean = clean[clean["quantity"] > 0]
    log(f"Quantities negativas filtradas: {neg_qty}, cero: {zero_qty}")
    
    # 5d. Unit price nulos → promedio del SKU
    null_prices = clean["unit_price"].isna().sum()
    if null_prices > 0:
        sku_avg_price = clean.groupby("sku")["unit_price"].transform("mean")
        clean["unit_price"] = clean["unit_price"].fillna(sku_avg_price)
        global_avg = clean["unit_price"].mean()
        clean["unit_price"] = clean["unit_price"].fillna(global_avg)
        log(f"Unit prices nulos rellenados con promedio del SKU: {null_prices}")
    
    # 5e. Redondear precios
    clean["unit_price"] = clean["unit_price"].round(2)
    
    # 5f. Normalizar channel
    clean["channel"] = clean["channel"].str.lower().str.strip()
    
    log(f"Filas: {original} originales → {len(clean)} limpias")
    
    return clean

# ─── 6. EXPORTAR CSVs LIMPIOS ────────────────────────────────────
def export_processed(customers, products, orders):
    """
    Exporta los datos limpios como CSVs en data/processed/.
    Así Power BI puede leer directamente los CSVs limpios.
    """
    separator("6. EXPORTACIÓN — CSVs Limpios (data/processed/)")
    
    customers.to_csv(os.path.join(PROCESSED_DIR, "customers_clean.csv"), index=False)
    log(f"customers_clean.csv: {len(customers)} filas")
    
    products.to_csv(os.path.join(PROCESSED_DIR, "products_clean.csv"), index=False)
    log(f"products_clean.csv: {len(products)} filas")
    
    orders.to_csv(os.path.join(PROCESSED_DIR, "orders_clean.csv"), index=False)
    log(f"orders_clean.csv: {len(orders)} filas")
    
    log(f"Directorio: {PROCESSED_DIR}")

# ─── 7. CARGA EN SQLITE ──────────────────────────────────────────
def load_to_sqlite(customers, products, orders):
    """Carga datos limpios en SQLite (idempotente)."""
    separator("7. CARGA EN SQLITE (Idempotente)")
    
    # Eliminar BD anterior
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        log("BD anterior eliminada (carga idempotente)")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Crear schema
    schema_path = os.path.join(BASE_DIR, "sql", "schema.sql")
    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()
    conn.executescript(schema_sql)
    log("Schema creado desde schema.sql")
    
    # Insertar datos
    customers.to_sql("customers", conn, if_exists="append", index=False)
    log(f"Customers cargados: {len(customers)} filas")
    
    products.to_sql("products", conn, if_exists="append", index=False)
    log(f"Products cargados: {len(products)} filas")
    
    orders.to_sql("orders", conn, if_exists="append", index=False)
    log(f"Orders cargados: {len(orders)} filas")
    
    # Vista enriquecida
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_orders_enriched AS
        SELECT
            o.order_id,
            o.customer_id,
            c.name AS customer_name,
            c.country,
            c.segment,
            o.sku,
            p.category,
            p.cost AS product_cost,
            p.active AS product_active,
            o.quantity,
            o.unit_price,
            ROUND(o.quantity * o.unit_price, 2) AS revenue,
            ROUND(o.quantity * (o.unit_price - p.cost), 2) AS margin,
            o.order_date,
            o.channel
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN products p ON o.sku = p.sku
    """)
    log("Vista v_orders_enriched creada")
    
    conn.commit()
    conn.close()
    log(f"BD guardada: {DB_PATH}")

# ─── 8. VALIDACIÓN ───────────────────────────────────────────────
def validate():
    """Validación post-carga con KPIs."""
    separator("8. VALIDACIÓN POST-CARGA")
    
    conn = sqlite3.connect(DB_PATH)
    
    for table in ["customers", "products", "orders"]:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        log(f"Tabla {table}: {count} registros")
    
    # KPIs
    result = conn.execute("""
        SELECT
            ROUND(SUM(quantity * unit_price), 2),
            COUNT(DISTINCT order_id),
            ROUND(SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT order_id), 0), 2)
        FROM orders
    """).fetchone()
    
    margin = conn.execute("""
        SELECT ROUND(SUM(o.quantity * (o.unit_price - COALESCE(p.cost, 0))), 2)
        FROM orders o LEFT JOIN products p ON o.sku = p.sku
    """).fetchone()[0]
    
    log(f"Ventas Totales:  ${result[0]:,.2f}")
    log(f"Margen Total:    ${margin:,.2f}")
    log(f"Num. Pedidos:    {result[1]}")
    log(f"Ticket Promedio: ${result[2]:,.2f}")
    
    conn.close()

# ─── MAIN ─────────────────────────────────────────────────────────
def main():
    global LOG_FILE
    
    print("\n" + "# " * 15)
    print("  RoseAmor — ETL Pipeline (Senior Edition)")
    print("# " * 15)
    
    start = datetime.now()
    ensure_dirs()
    
    # Abrir log
    log_path = os.path.join(LOGS_DIR, f"etl_run_{TIMESTAMP}.log")
    LOG_FILE = open(log_path, "w", encoding="utf-8")
    log(f"Inicio de ejecución: {TIMESTAMP}")
    log(f"Log: {log_path}")
    
    try:
        # 0. Archivar originales (NUNCA se tocan)
        archive_raw_files()
        
        # 1. Cargar (read-only)
        customers_raw, products_raw, orders_raw = load_raw_data()
        
        # 2. Perfilar (sin modificar)
        profile = profile_data(customers_raw, products_raw, orders_raw)
        
        # 3-5. Limpiar (trabaja con COPIAS)
        customers_clean = clean_customers(customers_raw)
        products_clean = clean_products(products_raw)
        orders_clean = clean_orders(orders_raw, products_clean)
        
        # 6. Exportar CSVs limpios
        export_processed(customers_clean, products_clean, orders_clean)
        
        # 7. Cargar en SQLite
        load_to_sqlite(customers_clean, products_clean, orders_clean)
        
        # 8. Validar
        validate()
        
        elapsed = (datetime.now() - start).total_seconds()
        separator(" ETL COMPLETADO EXITOSAMENTE")
        log(f"Tiempo total: {elapsed:.1f} segundos")
        log(f"Base de datos: {DB_PATH}")
        log(f"CSVs limpios: {PROCESSED_DIR}")
        log(f"Archivos originales preservados en: {RAW_DIR} (intactos)")
        log(f"Copia de seguridad en: {ARCHIVE_DIR}/{TIMESTAMP}/")
        
    except Exception as e:
        log(f"ERROR FATAL: {str(e)}", "ERROR")
        raise
    finally:
        LOG_FILE.close()

if __name__ == "__main__":
    main()
