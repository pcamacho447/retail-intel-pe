# Arquitectura Fase E1 — Ingesta, Capa Bronze y Benchmark (`retail-intel-pe`)

**Rol BMAD:** Architect  
**Versión:** `0.1.5`  
**Fecha:** 2026-11-13  
**Alcance:** Fase E1 + Hardening E1.5 exclusivamente (Ingesta multi-retailer en Rust para Plaza Vea, Metro, Wong y Tottus; persistencia Bronze inmutable en Parquet ZSTD de 29 columnas con deduplicación por `payload_hash`, validación GS1 Mod-10 para fallbacks EAN, enriquecimiento selectivo/cacheado de PDPs Tottus en `state/tottus_ean_cache.json`, DDL inicial PostgreSQL y auditoría DuckDB). Quedan fuera de E1 las reglas completas de normalización Silver (E2) y embeddings/matching (E3).

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
| VTEX API (PV, Metro,  |      | Falabella Listing + PDP   |
| Wong) _from/_to JSON  |      | (Tottus) __NEXT_DATA__ /  |
| + fallback EAN Mod-10 |      | JSON-LD + ean cache       |
+-----------+-----------+      +-------------+-------------+
            |                                |
            v                                v
   +--------+--------------------------------+--------+
   | rust-ingestor (tokio + reqwest + governor <=2r/s)|
   |  - Trait RetailerAdapter (Product 1:N SkuItem)   |
   |  - Exponential Backoff (max_retries=3)           |
   |  - CheckpointStore (JSON resume + page_size)     |
   |  - PayloadDeduplicator (SHA-256 seen_hashes)     |
   |  - TottusPdpCache (state/tottus_ean_cache.json)  |
   +------------------------+-------------------------+
                            |
            +---------------+---------------+
            v                               v
+-----------+--------------+    +-----------+--------------+
| Parquet Bronze (ZSTD-3)  |    | Run Metrics & Logs       |
| 29 cols (+ card_program) |    | metrics-<run_id>.json    |
| data/bronze/dt=YYYY-MM-DD|    | tracing JSON/structured  |
| /retailer=<name>/        |    | peak_ram_mb (/proc/self) |
| part-<run_id>.parquet    |    | wire/content bytes       |
+-----------+--------------+    +--------------------------+
            |
            v
+-----------+--------------+    +--------------------------+
| scripts/inspect_bronze.py|    | sql/postgres_schema.sql  |
| DuckDB SQL Audit         |    | extraction_runs +        |
| (SKU/Prod, EAN Mod-10,   |    | 5 tablas catálogo maestro|
|  1:N Fanout, Card Prog.) |    | (sin índice redundante)  |
+--------------------------+    +--------------------------+
```

---

## 3. Esquema Bronze Parquet (`BronzeSkuRow`, 29 columnas)

Compresión: `ZSTD(level=3)`. Particionado físico Hive-style: `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet`.

| Columna | Tipo Arrow | Nullable | Descripción |
| :--- | :--- | :---: | :--- |
| `retailer` | `Utf8` | No | `plazavea`, `metro`, `wong`, `tottus` |
| `market_id` | `Utf8` | No | Identificador de mercado (ej. `PE`) |
| `store_id` | `Utf8` | Sí | Tienda o zona resuelta |
| `sku_id` | `Utf8` | No | Identificador único de SKU en el retailer |
| `product_id` | `Utf8` | No | Identificador de producto padre (1:N) |
| `ean` | `Utf8` | Sí | Código EAN/GTIN reportado o resuelto por fallback Mod-10 / caché PDP (vacíos normalizados a `null`) |
| `seller_id` | `Utf8` | Sí | Seller marketplace o tienda propia |
| `raw_name` | `Utf8` | No | Nombre comercial del SKU/producto |
| `brand` | `Utf8` | Sí | Marca reportada |
| `category_path` | `Utf8` | Sí | Ruta jerárquica de categoría |
| `raw_description` | `Utf8` | Sí | Descripción HTML/texto crudo |
| `product_url` | `Utf8` | Sí | URL pública del producto |
| `image_url` | `Utf8` | Sí | URL principal de imagen |
| `list_price` | `Float64` | Sí | Precio lista / regular |
| `selling_price` | `Float64` | Sí | Precio de venta online |
| `card_price` | `Float64` | Sí | Precio tarjeta/membresía (`CMR`, `Oh!`, `Cencosud`, `Wong Prime`) |
| `card_program` | `Utf8` | Sí | Programa/tarjeta que otorga `card_price` (`Oh!`, `Cencosud`, `Wong Prime`, `CMR`) |
| `available_qty` | `Int64` | Sí | Stock disponible reportado (o límite `maxOnCartAmount` / indicador `InStock`/`OutOfStock` en Tottus) |
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

## 4. Contratos de Adaptadores, Caché y Resiliencia

- **Rate Limiting:** `governor::RateLimiter` configurado a máximo **2 req/s** por instancia/dominio de adaptador (incluye consultas selectivas a PDPs de Tottus).
- **Retry / Backoff:** Hasta 3 reintentos con backoff exponencial (`500ms * 2^attempt`) para HTTP `429`, `5xx` o errores transitorios de red.
- **Validación GS1 Mod-10 (`validate_gtin_mod10`):** Todo fallback de `ean` (`items[].referenceId`, `productReference`, `productReferenceCode`, especificaciones VTEX, JSON-LD `gtin13` en Tottus) se acepta únicamente si tiene 8, 12, 13 o 14 dígitos numéricos, no es todo-ceros y supera el checksum GS1 Mod-10.
- **Enriquecimiento Selectivo Tottus + Caché (`state/tottus_ean_cache.json`):** Activado vía `--enrich-tottus-pdp --tottus-ean-cache <path>`, consulta la PDP únicamente para SKUs sin entrada previa en caché, extrayendo disponibilidad (`InStock`/`maxOnCartAmount`) y GTIN-13 Mod-10 si está disponible, sin sobrescribir jamás un EAN válido previo cuando el PDP retorne `"gtin13": "null"`.
- **Checkpoints y Colisiones:** Persistidos en `data/bronze/checkpoints/<retailer>_<category_slug>.json` tras finalizar Parquet, incluyendo `page_size` para reanudación consistente y desambiguación automática con sufijo hash ante colisión de slug.
- **Métricas Emitidas:** `products_recovered`, `skus_recovered`, `pages_fetched`, `duplicates_detected`, `requests_ok`, `requests_error`, `avg_latency_ms`, `mb_transferred` (`Content-Length` en red cuando existe o longitud de bytes recibidos), `total_duration_secs`, `peak_ram_mb`.

---

## 5. Esquema Relacional PostgreSQL (`sql/postgres_schema.sql`)

Incluye `extraction_runs` para telemetría de corridas E1 y las 5 tablas del catálogo maestro (con soporte SCD Tipo 2 en `bridge_sku_trade_item` e integridad referencial para E2/E3):
- `extraction_runs`
- `dim_product_family`
- `dim_trade_item`
- `dim_comparison_group`
- `bridge_sku_trade_item`
- `match_review_log`
