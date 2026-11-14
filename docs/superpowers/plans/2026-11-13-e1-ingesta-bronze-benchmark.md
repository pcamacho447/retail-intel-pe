# E1 Ingesta, Bronze y Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Construir e inicializar el repositorio `retail-intel-pe` con la Fase E1 completa: ingesta multi-retailer en Rust (`VtexAdapter` y `FalabellaListingAdapter`), persistencia Parquet ZSTD particionada con deduplicación por `payload_hash`, DDL inicial PostgreSQL y script de auditoría DuckDB validado con corrida piloto real.

**Architecture:** Workspace Cargo en `rust-ingestor/` con trait común `RetailerAdapter` que separa `Product` de `SkuItem` (1:N), rate limiting con `governor` (máx. 2 req/s), backoff exponencial, checkpoints JSON y escritor Arrow/Parquet ZSTD. En paralelo, `sql/postgres_schema.sql` define las tablas operativas y maestras en PostgreSQL y `scripts/inspect_bronze.py` usa DuckDB para auditar métricas de calidad sobre los Parquet generados.

**Tech Stack:** Rust (`tokio`, `reqwest`, `governor`, `serde`, `serde_json`, `arrow`, `parquet`, `chrono`, `sha2`, `uuid`, `tracing`, `clap`), Python (`duckdb`, `polars`, `pyarrow`, `pytest`), PostgreSQL DDL.

**Spec:** `docs/architecture-e1.md` y `docs/stories/e1-*.md`

## Global Constraints
- `rust-ingestor/` usa dependencias requeridas: `tokio`, `reqwest`, `governor`, `serde`, `serde_json`, `arrow`, `parquet`, `chrono`, `sha2`, `uuid`, `tracing`, `clap`.
- Particionado Bronze estricto: `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet` con compresión ZSTD.
- Deduplicación de `raw_payload` por `payload_hash` (SHA-256 hex).
- Rate limiting por dominio máximo `2 req/s` con `governor` y backoff exponencial con límite de reintentos.
- TDD obligatorio: toda función pública/módulo tiene tests escritos antes de implementar y validados en rojo -> verde.
- No implementar reglas completas de E2 ni embeddings de E3.

## Review Focus
- Paginación VTEX cuando `_from`/`_to` devuelve menos resultados que `page_size` o lista vacía `[]`: debe detener el bucle sin bucle infinito y marcar el checkpoint como `completed: true`.
- Productos VTEX con `ean: ""` (cadena vacía) o `sellers: []` (sin oferta): `ean` vacío debe normalizarse a `None` (no contar como EAN válido) y precios faltantes deben quedar en `None`.
- Extracción `__NEXT_DATA__` de Tottus cuando el HTML no contiene el script (ej. bloqueo WAF/CAPTCHA o cambio de markup) o cuando `prices` trae formatos con comas o múltiples elementos: debe retornar error descriptivo con código HTTP sin hacer panic.
- Deduplicación entre corridas: una segunda corrida sobre los mismos productos debe detectar `duplicates_detected > 0`, escribir `is_duplicate_payload = true` y omitir `raw_payload = None` en el Parquet.
- Auditoría DuckDB sobre Parquet con columnas completamente nulas (ej. `card_price` todo NULL en una muestra) o mezcla de archivos entre retailers: `scripts/inspect_bronze.py` no debe fallar por divisiones entre cero ni inferencia de tipo nula.

---

### Task 1: DDL PostgreSQL (`sql/postgres_schema.sql`) y Auditoría DuckDB (`scripts/inspect_bronze.py`) [DataSchemaAgent]

**Files:**
- Create: `sql/postgres_schema.sql`
- Create: `tests/test_inspect_bronze.py`
- Create: `scripts/inspect_bronze.py`

**Interfaces:**
- Produces: `sql/postgres_schema.sql` (DDL validable/idempotente), `scripts/inspect_bronze.py` CLI (`--bronze-dir`, `--output-json`, `--output-md`) y función `audit_bronze(bronze_dir: Path) -> dict`.

- [ ] **Step 1: Escribir `sql/postgres_schema.sql` con `extraction_runs` y las 5 tablas maestras (`dim_product_family`, `dim_trade_item`, `dim_comparison_group`, `bridge_sku_trade_item`, `match_review_log`) e índices SCD2**
- [ ] **Step 2: Escribir tests TDD en `tests/test_inspect_bronze.py` verificando métricas de productos vs SKUs, cobertura EAN (ignorando `""` y NULL), tasa de duplicados, distribución de precios y manejo de directorios vacíos/columnas NULL**
- [ ] **Step 3: Ejecutar `.venv/bin/pytest tests/test_inspect_bronze.py -v` y confirmar fallo inicial (RED)**
- [ ] **Step 4: Implementar `scripts/inspect_bronze.py` usando `duckdb` para pasar todos los tests**
- [ ] **Step 5: Ejecutar `.venv/bin/pytest tests/test_inspect_bronze.py -v` y confirmar 100% PASS (GREEN)**

---

### Task 2: Workspace `rust-ingestor` Core Domain, Trait `RetailerAdapter`, Deduplicación y Checkpoints [RustIngestorAgent]

**Files:**
- Create: `rust-ingestor/Cargo.toml`
- Create: `rust-ingestor/crates/ingestor/Cargo.toml`
- Create: `rust-ingestor/crates/ingestor/src/lib.rs`
- Create: `rust-ingestor/crates/ingestor/src/domain.rs`
- Create: `rust-ingestor/crates/ingestor/src/adapter.rs`
- Create: `rust-ingestor/crates/ingestor/src/dedup.rs`
- Create: `rust-ingestor/crates/ingestor/src/checkpoint.rs`

**Interfaces:**
- Produces: `Product`, `SkuItem`, `BronzeSkuRow`, `RetailerAdapter`, `PayloadDeduplicator`, `CheckpointStore`, `RunMetrics`.

- [ ] **Step 1: Crear workspace Cargo con todas las dependencias especificadas**
- [ ] **Step 2: Escribir tests unitarios primero en `domain.rs`, `dedup.rs`, `checkpoint.rs` (expansión 1:N, SHA-256 dedup entre corridas, persistencia/reanudación de checkpoints) y confirmar fallo (RED)**
- [ ] **Step 3: Implementar `domain.rs`, `adapter.rs`, `dedup.rs` y `checkpoint.rs` hasta pasar `cargo test` (GREEN)**

---

### Task 3: Adaptadores `VtexAdapter` y `FalabellaListingAdapter` con `governor` y Backoff Exponencial [RustIngestorAgent]

**Files:**
- Create: `rust-ingestor/crates/ingestor/src/http.rs`
- Create: `rust-ingestor/crates/ingestor/src/adapters/mod.rs`
- Create: `rust-ingestor/crates/ingestor/src/adapters/vtex.rs`
- Create: `rust-ingestor/crates/ingestor/src/adapters/falabella.rs`

**Interfaces:**
- Consumes: `domain::Product`, `domain::SkuItem`, `adapter::RetailerAdapter`.
- Produces: `RateLimitedHttpClient`, `VtexAdapter`, `FalabellaListingAdapter`.

- [ ] **Step 1: Escribir tests unitarios con fixtures reales VTEX (multi-SKU, `ean: ""`, ofertas) y Tottus `__NEXT_DATA__` (`normalPrice`, `internetPrice`, `cmrPrice`, error si falta script) y verificar fallo (RED)**
- [ ] **Step 2: Implementar `http.rs` (`governor` <= 2 req/s, backoff exponencial, conteo de latencia y bytes), `adapters/vtex.rs` y `adapters/falabella.rs`**
- [ ] **Step 3: Ejecutar `cargo test` y verificar verde (GREEN)**

---

### Task 4: Parquet Bronze ZSTD Writer, Métricas y CLI [RustIngestorAgent]

**Files:**
- Create: `rust-ingestor/crates/ingestor/src/storage/mod.rs`
- Create: `rust-ingestor/crates/ingestor/src/storage/parquet_writer.rs`
- Create: `rust-ingestor/crates/ingestor/src/metrics.rs`
- Create: `rust-ingestor/crates/ingestor/src/main.rs`

**Interfaces:**
- Consumes: `BronzeSkuRow`, `RetailerAdapter`, `PayloadDeduplicator`, `CheckpointStore`.
- Produces: binario `ingestor` escribiendo `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet` y `metrics-<run_id>.json`.

- [ ] **Step 1: Escribir tests TDD para `parquet_writer.rs` (esquema Arrow, compresión ZSTD, lectura de vuelta) y `metrics.rs` (pico de RAM, latencia promedio, MB transferidos)**
- [ ] **Step 2: Implementar `parquet_writer.rs`, `metrics.rs` y `main.rs` con `clap` y `tracing`**
- [ ] **Step 3: Ejecutar `cargo test` y `cargo build --release`**

---

### Task 5: Corrida Piloto Real, Auditoría DuckDB y Revisión QA (`verification-before-completion` + `requesting-code-review`)

- [ ] **Step 1: Ejecutar piloto real en Plaza Vea (`lacteos,abarrotes`), Metro (`lacteos,abarrotes`) y Tottus (`leche,abarrotes`)**
- [ ] **Step 2: Ejecutar `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze --output-md docs/qa-report-e1.md`**
- [ ] **Step 3: Despachar `QABenchmarkAgent` para revisión de código y auditoría de métricas reales en `docs/qa-report-e1.md`**
- [ ] **Step 4: Commit inicial del repositorio `retail-intel-pe`**
