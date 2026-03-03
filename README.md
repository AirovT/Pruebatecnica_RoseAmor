# Ingeniería de Datos y Analytics - Proyecto RoseAmor

Este repositorio contiene la solución técnica desarrollada para RoseAmor, enfocada en la transformación de datos operativos de ventas en información estratégica. El sistema integra un pipeline ETL automatizado, un repositorio de datos relacional, un dashboard ejecutivo en Power BI y una herramienta web para la gestión de operaciones en tiempo real.

## Componentes del Sistema

- **Pipeline ETL**: Motor de transformación en Python diseñado para la limpieza, validación y canonización de datos.
- **Data Warehouse Relacional**: Implementación en SQLite con integridad referencial y vistas optimizadas para reporting.
- **Dashboard Ejecutivo**: Sistema de visualización interactivo en Power BI para el monitoreo de KPIs críticos.
- **Portal Operativo**: Interfaz web desarrollada en Flask para el registro de transacciones y visualización de métricas en vivo.

## Arquitectura de la Solución

El flujo de datos sigue un modelo de capas para garantizar la trazabilidad:

1. **Capa Raw**: Almacenamiento inmutable de los archivos fuente (CSV).
2. **Capa de Procesamiento**: Procesos de Python que aplican reglas de negocio, corrección de tipos y deduplicación.
3. **Capa de Consumo**: Tablas finales y vistas SQL preparadas para el consumo por parte de analistas y sistemas operativos.

## Detalles de Implementación

### Gestión de Calidad de Datos
El pipeline ETL realiza un perfilado profundo para resolver:
- Estandarización de datos geográficos y segmentos de clientes nulos.
- Corrección de anomalías numéricas (como costos negativos detectados en fuente).
- Imputación de precios unitarios faltantes basados en promedios históricos por SKU.
- Eliminación de registros con fechas inválidas o identificadores duplicados.

### Estructura de Base de Datos
Se implementó un esquema de estrella simplificado para optimizar el rendimiento:
- **Customers**: Registro maestro de clientes corporativos.
- **Products**: Catálogo de productos activos con trazabilidad de costos operativos.
- **Orders**: Tabla de hechos con todas las transacciones históricas y restricciones de integridad.

## Instrucciones de Ejecución

### Requisitos Previos
- Python 3.10 o superior.
- Power BI Desktop.

### Configuración e Inicio

1. **Preparación del Entorno**:
   ```powershell
   pip install -r etl/requirements.txt
   ```

2. **Ejecución del Pipeline ETL**:
   Procesa los datos brutos y genera la base de datos relacional:
   ```powershell
   python etl/etl_pipeline.py
   ```

3. **Interfaz de Gestión Web**:
   Inicia el servidor backend para el registro de pedidos:
   ```powershell
   python web/app.py
   ```
   Acceso local: `http://localhost:5000`

4. **Visualización de Reportes**:
   Abrir el archivo `RoseAmor_Dashboard.pbix` en Power BI Desktop. Utilizar el botón "Actualizar" para sincronizar con la base de datos generada.

## Despliegue en Repositorio (Git)

Para realizar la entrega formal del proyecto, siga el flujo de trabajo estándar:

1. **Inicialización local**:
   ```bash
   git init
   git add .
   git commit -m "Initial commit: Proyecto completo RoseAmor"
   ```

2. **Vinculación con GitHub**:
   Cree un repositorio en GitHub y ejecute:
   ```bash
   git remote add origin [URL-DE-TU-REPOSITORIO]
   git branch -M main
   git push -u origin main
   ```

## Resultado del Reporte Ejecutivo

A continuación, se presenta la visualización final del dashboard estratégico diseñado:

![Resultado Final RoseAmor](RoseAmor_Dashboard.png)

---
*Desarrollado para la Evaluación Técnica de RoseAmor - 2025*
