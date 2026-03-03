"""
RoseAmor — Registro de Pedidos (Web App)
Autor: Jairo Torres
Descripción: App Flask para registrar pedidos y visualizar datos.
"""

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash
import sqlite3
import os
from datetime import datetime

# ─── Configuración ───────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, "data", "roseamor.db")

app = Flask(__name__)
app.secret_key = "roseamor-2025"

# ─── Helpers ──────────────────────────────────────────────────────
def get_db():
    """Obtiene conexión a SQLite."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def generate_order_id():
    """Genera un nuevo order_id autoincremental."""
    conn = get_db()
    result = conn.execute("""
        SELECT order_id FROM orders 
        ORDER BY order_id DESC LIMIT 1
    """).fetchone()
    conn.close()
    
    if result:
        last_num = int(result["order_id"].replace("O", ""))
        return f"O{last_num + 1:06d}"
    return "O000001"


# ─── Rutas ────────────────────────────────────────────────────────

@app.route("/")
def index():
    """Página principal con formulario y tabla de pedidos recientes."""
    conn = get_db()
    
    # Obtener clientes para dropdown
    customers = conn.execute(
        "SELECT customer_id, name FROM customers ORDER BY name"
    ).fetchall()
    
    # Obtener productos para dropdown
    products = conn.execute(
        "SELECT sku, category, cost FROM products WHERE active = '1' ORDER BY sku"
    ).fetchall()
    
    # Últimos 20 pedidos con datos enriquecidos
    recent_orders = conn.execute("""
        SELECT 
            o.order_id,
            o.customer_id,
            c.name AS customer_name,
            o.sku,
            p.category,
            o.quantity,
            o.unit_price,
            ROUND(o.quantity * o.unit_price, 2) AS total,
            o.order_date,
            o.channel
        FROM orders o
        LEFT JOIN customers c ON o.customer_id = c.customer_id
        LEFT JOIN products p ON o.sku = p.sku
        ORDER BY o.order_date DESC, o.order_id DESC
        LIMIT 20
    """).fetchall()
    
    # KPIs
    kpis = conn.execute("""
        SELECT
            ROUND(SUM(quantity * unit_price), 2) AS ventas_totales,
            COUNT(DISTINCT order_id) AS num_pedidos,
            ROUND(SUM(quantity * unit_price) / NULLIF(COUNT(DISTINCT order_id), 0), 2) AS ticket_promedio
        FROM orders
    """).fetchone()
    
    margin = conn.execute("""
        SELECT ROUND(SUM(o.quantity * (o.unit_price - COALESCE(p.cost, 0))), 2) AS margen
        FROM orders o LEFT JOIN products p ON o.sku = p.sku
    """).fetchone()
    
    new_order_id = generate_order_id()
    
    conn.close()
    
    return render_template(
        "index.html",
        customers=customers,
        products=products,
        recent_orders=recent_orders,
        kpis=kpis,
        margin=margin,
        new_order_id=new_order_id,
        channels=["retail", "ecommerce", "wholesale", "export"]
    )


@app.route("/order", methods=["POST"])
def create_order():
    """Crea un nuevo pedido."""
    try:
        order_id = request.form["order_id"].strip()
        customer_id = request.form["customer_id"]
        sku = request.form["sku"]
        quantity = int(request.form["quantity"])
        unit_price = float(request.form["unit_price"])
        order_date = request.form["order_date"]
        channel = request.form["channel"]
        
        # Validaciones
        errors = []
        if not order_id:
            errors.append("Order ID es requerido")
        if quantity <= 0:
            errors.append("Cantidad debe ser mayor a 0")
        if unit_price <= 0:
            errors.append("Precio unitario debe ser mayor a 0")
        if not order_date:
            errors.append("Fecha es requerida")
        else:
            # Validar que la fecha no sea futura
            selected_date = datetime.strptime(order_date, '%Y-%m-%d').date()
            if selected_date > datetime.now().date():
                errors.append("La fecha no puede ser futura")
            
        if errors:
            flash(" | ".join(errors), "error")
            return redirect(url_for("index"))
        
        # Formatear fecha
        order_date_formatted = f"{order_date} 00:00:00"
        
        conn = get_db()
        conn.execute("""
            INSERT INTO orders (order_id, customer_id, sku, quantity, unit_price, order_date, channel)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (order_id, customer_id, sku, quantity, unit_price, order_date_formatted, channel))
        conn.commit()
        conn.close()
        
        flash(f"✅ Pedido {order_id} registrado exitosamente", "success")
        
    except sqlite3.IntegrityError:
        flash(f"❌ El Order ID {order_id} ya existe", "error")
    except ValueError as e:
        flash(f"❌ Error de validación: {str(e)}", "error")
    except Exception as e:
        flash(f"❌ Error inesperado: {str(e)}", "error")
    
    return redirect(url_for("index"))


@app.route("/api/customers")
def api_customers():
    """API: Lista de clientes para dropdowns."""
    conn = get_db()
    customers = conn.execute(
        "SELECT customer_id, name, country, segment FROM customers ORDER BY name"
    ).fetchall()
    conn.close()
    return jsonify([dict(c) for c in customers])


@app.route("/api/products")
def api_products():
    """API: Lista de productos activos para dropdowns."""
    conn = get_db()
    products = conn.execute(
        "SELECT sku, category, cost FROM products WHERE active = '1' ORDER BY sku"
    ).fetchall()
    conn.close()
    return jsonify([dict(p) for p in products])


# ─── Main ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not os.path.exists(DB_PATH):
        print("❌ Base de datos no encontrada. Ejecuta primero: python etl/etl_pipeline.py")
        exit(1)
    
    print("🌹 RoseAmor — Registro de Pedidos")
    print(f"   Base de datos: {DB_PATH}")
    print(f"   Servidor: http://localhost:5000")
    print()
    
    app.run(debug=True, host="0.0.0.0", port=5000)
