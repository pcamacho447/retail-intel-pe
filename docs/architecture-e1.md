# Arquitectura Fase E1 — Ingesta, Capa Bronze y Benchmark (`retail-intel-pe`)

**Rol BMAD:** Architect  
**Versión:** `0.1.0`  
**Fecha:** 2026-11-13  
**Alcance:** Fase E1 exclusivamente (Ingesta multi-retailer en Rust, persistencia Bronze inmutable en Parquet ZSTD con deduplicación por `payload_hash`, DDL inicial PostgreSQL y auditoría DuckDB). Quedan fuera de E1 las reglas completas de normalización Silver (E2) y embeddings/matching (E3).

---

## 1. Principios Arquitectónicos Aplicados en E1

1. **Inmutabilidad Bronze:** Las observaciones crudas extraídas desde las plataformas e-commerce se almacenan en particiones append-only `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet` sin mutar registros previos.
2. **Separación 1:N (`Product` vs. `SkuItem`):** Todo adaptador implementa `RetailerAdapter` y modela explícitamente un `Product` padre con `Vec<SkuItem>` hijos, expandiendo a nivel SKU (`BronzeSkuRow`) únicamente al materializar la capa Bronze.
3. **Observación Contextualizada:** Cada fila conserva `(retailer, market_id, store_id, sku_id, product_id, postal_code, sales_channel, scraped_at, extraction_run_id, extractor_version, source_endpoint, http_status, payload_hash)`.
4. **Deduplicación de `raw_payload` por `payload_hash` (SHA-256):** Se calcula el hash SHA-256 del payload crudo por producto. Si el `payload_hash` ya fue registrado en el blob store (`data/bronze/blobs/seen_hashes.json`), `is_duplicate_payload = true` y `raw_payload = None` en el Parquet para evitar almacenar JSON duplicado sin cambios diarios.

---

## 2. Topología de Componentes E1

```text
+-----------------------+      +---------------------------+
| VTEX API (PV, Metro,  |      | Falabella Listing (Tottus)|
| Wong) _from/_to JSON  |      | __NEXT_DATA__ / BFF JSON  |
+-----------+-----------+      +-------------+-------------+
            |                                |
            v                                v
   +--------+--------------------------------+--------+
   | rust-ingestor (tokio + reqwest + governor <=2r/s)|
   |  - Trait RetailerAdapter (Product 1:N SkuItem)   |
   |  - Exponential Backoff (max_retries=3)           |
   |  - CheckpointStore (JSON resume por categoría)   |
   |  - PayloadDeduplicator (SHA-256 seen_hashes)     |
   +------------------------+-------------------------+
                            |
            +---------------+---------------+
            v                               v
+-----------+--------------+    +-----------+--------------+
| Parquet Bronze (ZSTD-3)  |    | Run Metrics & Logs       |
| data/bronze/dt=YYYY-MM-DD|    | metrics-<run_id>.json    |
| /retailer=<name>/        |    | tracing JSON/structured  |
| part-<run_id>.parquet    |    | peak_ram_mb (/proc/self) |
+-----------+--------------+    +--------------------------+
            |
            v
+-----------+--------------+    +--------------------------+
| scripts/inspect_bronze.py|    | sql/postgres_schema.sql  |
| DuckDB SQL Audit         |    | extraction_runs +        |
| (SKU/Prod, EAN, Dups, S/)|    | 5 tablas catálogo maestro|
+--------------------------+    +--------------------------+
```

---

## 3. Esquema Bronze Parquet (`BronzeSkuRow`)

Compresión: `ZSTD(level=3)`. Particionado físico Hive-style: `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet`.

| Columna | Tipo Arrow | Nullable | Descripción |
| :--- | :--- | :---: | :--- |
| `retailer` | `Utf8` | No | `plazavea`, `metro`, `wong`, `tottus` |
| `market_id` | `Utf8` | No | Identificador de mercado (ej. `PE`) |
| `store_id` | `Utf8` | Sí | Tienda o zona resuelta |
| `sku_id` | `Utf8` | No | Identificador único de SKU en el retailer |
| `product_id` | `Utf8` | No | Identificador de producto padre (1:N) |
| `ean` | `Utf8` | Sí | Código EAN/GTIN reportado (vacíos normalizados a `null`) |
| `seller_id` | `Utf8` | Sí | Seller marketplace o tienda propia |
| `raw_name` | `Utf8` | No | Nombre comercial del SKU/producto |
| `brand` | `Utf8` | Sí | Marca reportada |
| `category_path` | `Utf8` | Sí | Ruta jerárquica de categoría |
| `raw_description` | `Utf8` | Sí | Descripción HTML/texto crudo |
| `product_url` | `Utf8` | Sí | URL pública del producto |
| `image_url` | `Utf8` | Sí | URL principal de imagen |
| `list_price` | `Float64` | Sí | Precio lista / regular |
| `selling_price` | `Float64` | Sí | Precio de venta online |
| `card_price` | `Float64` | Sí | Precio tarjeta/membresía (CMR, Oh!, Cencosud, Teaser) |
| `available_qty` | `Int64` | Sí | Stock disponible reportado |
| `currency` | `Utf8` | No | Moneda (`PEN`) |
| `sales_channel` | `Utf8` | No | Canal de venta (`ecommerce`) |
| `postal_code` | `Utf8` | Sí | Código postal/ubigeo de consulta |
| `scraped_at` | `Timestamp(Millisecond, UTC)` | No | Timestamp UTC de extracción |
| `extraction_run_id` | `Utf8` | No | UUID v4 de la corrida |
| `extractor_version` | `Utf8` | No | Versión semántica del adaptador |
| `source_endpoint` | `Utf8` | No | URL exacta consultada |
| `http_status` | `UInt16` | No | Código HTTP recibido |
| `payload_hash` | `Utf8` | No | SHA-256 hex del payload crudo del producto |
| `is_duplicate_payload` | `Boolean` | No | `true` si `payload_hash` ya existía |
| `raw_payload` | `Utf8` | Sí | JSON crudo del producto (`null` si duplicado) |

---

## 4. Contratos de Adaptadores y Resiliencia

- **Rate Limiting:** `governor::RateLimiter` configurado a máximo **2 req/s** por instancia/dominio de adaptador.
- **Retry / Backoff:** Hasta 3 reintentos con backoff exponencial (`500ms * 2^attempt`) para HTTP `429`, `5xx` o errores transitorios de red.
- **Checkpoints:** Persistidos en `data/bronze/checkpoints/<retailer>_<category_slug>.json` tras cada página exitosa, permitiendo `--resume` idempotente sin repetir páginas ya completadas.
- **Métricas Emitidas:** `products_recovered`, `skus_recovered`, `pages_fetched`, `duplicates_detected`, `requests_ok`, `requests_error`, `avg_latency_ms`, `mb_transferred`, `total_duration_secs`, `peak_ram_mb`.

---

## 5. Esquema Relacional PostgreSQL (`sql/postgres_schema.sql`)

Incluye `extraction_runs` para telemetría de corridas E1 y las 5 tablas del catálogo maestro (con soporte SCD Tipo 2 en `bridge_sku_trade_item` e integridad referencial para E2/E3):
- `extraction_runs`
- `dim_product_family`
- `dim_trade_item`
- `dim_comparison_group`
- `bridge_sku_trade_item`
- `match_review_log`
