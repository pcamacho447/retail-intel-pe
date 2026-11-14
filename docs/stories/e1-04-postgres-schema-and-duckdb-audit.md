# Story E1-04: Esquema PostgreSQL y Auditoría Bronze con DuckDB

**Rol BMAD:** PM / SM  
**Prioridad:** P0  
**Dependencias:** Story E1-03

## Descripción
Definir el DDL inicial en PostgreSQL para trazabilidad de corridas y catálogo maestro, e implementar el script Python/DuckDB `scripts/inspect_bronze.py` para auditar los archivos Parquet Bronze generados.

## Criterios de Aceptación
1. **DDL PostgreSQL (`sql/postgres_schema.sql`):**
   - Crea `extraction_runs` y las 5 tablas del catálogo maestro (`dim_product_family`, `dim_trade_item`, `dim_comparison_group`, `bridge_sku_trade_item`, `match_review_log`) con llaves primarias, foráneas, constraints e índices SCD Tipo 2.
2. **Auditoría DuckDB (`scripts/inspect_bronze.py` + `tests/test_inspect_bronze.py`):**
   - Consulta vía `duckdb` todos los Parquet bajo `data/bronze/dt=*/retailer=*/part-*.parquet`.
   - Reporta por retailer y agregado total: productos únicos vs SKUs, ratio SKU/Producto, cobertura de EAN no nulos (`%`), tasa de duplicados (`payload_hash` y `raw_payload IS NULL`), distribución de precios (`list_price`, `selling_price`, `card_price` min/p25/p50/mean/p75/max) y disponibilidad de stock.
   - Verificado con suite `pytest` usando datos sintéticos antes de auditar corridas reales.
