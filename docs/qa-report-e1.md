# Reporte QA y Auditoría Benchmark — Fase E1 (`retail-intel-pe`)

- **Fecha de auditoría:** 2026-11-14 (re-revisión post-remediación sobre corte piloto `dt=2026-11-14`)
- **Rol:** `QABenchmarkAgent` (Senior QA & Code Reviewer)
- **Metodología aplicada:** `requesting-code-review` + `verification-before-completion` contra `docs/architecture-e1.md` y `docs/stories/e1-01` a `e1-04`
- **Dictamen QA:** **`APPROVED / SHIP`** *(0 Critical · 0 Important abiertos · 2 Important resueltos · 7 Minor no bloqueantes)*

> **Resumen ejecutivo del dictamen:** Tras la remediación TDD de los 2 hallazgos `Important` (`IMP-01`, `IMP-02`) y de los ajustes `Minor` priorizados (`MIN-02`, `MIN-03`, `MIN-04` `sync_all()`, `MIN-05`, `MIN-09`), la suite Rust completa pasa **42/42** (`38` lib + `4` bin, `+5` tests TDD nuevos), `cargo fmt --check` y `cargo clippy --all-targets -- -D warnings` pasan con **0 errores/warnings**, `pytest` pasa **8/8**, y la re-corrida piloto en vivo (`plazavea`, `metro`, `tottus`) mantiene consistencia exacta de **100%** entre `metrics-*.json`, `checkpoints/*.json`, `seen_hashes.json` (249 hashes) y los 3 Parquet ZSTD-3 (250 filas, 249 productos, 250 SKUs). Los puntos `Minor` remanentes quedan documentados como mejoras no bloqueantes para E2/escalamiento.

---

## 1. Resumen de Suites y Controles Verificados

| Verificación | Comando Ejecutado / Alcance | Resultado | Evidencia |
| :--- | :--- | :--- | :--- |
| **Rust Formatter (`rustfmt`)** | `cd rust-ingestor && cargo fmt --check` | **PASS (`exit 0`)** | 0 diferencias de formato en todo el workspace Cargo. |
| **Rust Linter (`clippy`)** | `cd rust-ingestor && cargo clippy --all-targets -- -D warnings` | **PASS (`0` warnings)** | Compilación limpia sin advertencias ni errores en targets lib/bin/tests. |
| **Rust Unit & Integration Tests** | `cd rust-ingestor && cargo test` | **PASS (`42/42`)** | `38 passed` en `src/lib.rs` + `4 passed` en `src/main.rs`; `0 failed`, `0 ignored` (`1.00s`). |
| **Release Binary** | `ls -lh rust-ingestor/target/release/ingestor` | **PRESENT** | Binario recompilado (`15M`, `nov 13 19:41`). |
| **Python DuckDB Audit Suite** | `.venv/bin/pytest -v` | **PASS (`8/8`)** | `8 passed in 0.56s` en `tests/test_inspect_bronze.py`. |
| **DuckDB Bronze Audit (Re-corrida Piloto)** | `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze` | **PASS (`exit 0`)** | 3 particiones Parquet escaneadas (`250` filas, `249` productos, `250` SKUs), compresión `ZSTD`, esquema de 28 columnas verificado. |
| **PostgreSQL DDL (`sql/postgres_schema.sql`)** | Inspección estática línea a línea vs PostgreSQL 14 / BMAD E1-04 | **VERIFICADO (Estático)** | `psql` 14.24 disponible en host (`/var/run/postgresql:5432`), sin credenciales/BD configuradas en el workspace E1; DDL idempotente (`IF NOT EXISTS`), PKs, FKs, constraints e índices SCD2 validados estáticamente. |

---

## 2. Telemetría Operativa de Re-corrida Piloto (`dt=2026-11-14`)

Datos consolidados desde los `metrics-*.json` emitidos tras la recompilación release y contrastados contra `data/bronze/checkpoints/*.json`, `data/bronze/blobs/seen_hashes.json` y los metadatos Parquet reales:

| retailer | extraction_run_id | status | products_recovered | skus_recovered | pages_fetched | duplicates_detected | requests_ok | requests_error | avg_latency_ms | mb_transferred | total_duration_secs | peak_ram_mb |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `metro` | `0279a558-c83e-428d-a564-a345749dd635` | `success` | 80 | 81 | 4 | 0 | 4 | 0 | 385.57 | 2.8560 | 1.91 | 28.57 |
| `plazavea` | `b374f6cf-1762-4c6f-9dae-f7998fb1f2eb` | `success` | 73 | 73 | 4 | 0 | 4 | 0 | 227.73 | 0.9235 | 1.68 | 18.02 |
| `tottus` | `23fb4813-c9cb-4ae9-87ba-fc41a03498a3` | `success` | 96 | 96 | 2 | 0 | 2 | 0 | 1857.74 | 4.4384 | 3.73 | 21.07 |
| **TOTAL / AGG** | `3 corridas` | `3/3 success` | **249** | **250** | **10** | **0** | **10** | **0** | **—** | **8.2179** | **—** | **máx. 28.57** |

### Consistencia Artefactos vs. Métricas y Checkpoints

- **Parquet vs. `metrics-*.json`:** Exactitud `100%` en los 3 retailers (`metro`: 81 filas / 80 `product_id` / 81 `sku_id`; `plazavea`: 73 / 73 / 73; `tottus`: 96 / 96 / 96), con `started_at`, `finished_at`, `status: "success"`, `error: null`, `extractor_version` y `extraction_run_id` alineados.
- **Compresión y tamaño Parquet en disco:**
  - `metro`: `436K` en disco (`3,169,168` bytes sin comprimir, `ZSTD`, 1 row group, 28 columnas).
  - `plazavea`: `150K` en disco (`1,032,899` bytes sin comprimir, `ZSTD`, 1 row group, 28 columnas).
  - `tottus`: `36K` en disco (`158,515` bytes sin comprimir, `ZSTD`, 1 row group, 28 columnas).
- **Blob Store SHA-256 (`data/bronze/blobs/seen_hashes.json`):** Contiene exactamente `249` hashes SHA-256 distintos, con igualdad exacta (`seen_hashes == parquet_distinct_hashes`) frente a los `249` `payload_hash` distintos en Parquet.
- **Rate limiting `governor` observado en vivo:** En `metro` (`1.91s` para 4 req) y `plazavea` (`1.68s` para 4 req), el pacing con `allow_burst(1)` a `2 req/s` impuso `>= 1.5s` de intervalo mínimo + latencia de red; en `tottus` (`3.73s` para 2 req con `1857.74ms` avg), la latencia de SSR superó los `500ms` entre peticiones.
- **Estado de `data/bronze/checkpoints/*.json`:**
  - `metro_lacteos.json`: `next_offset=40`, `pages_completed=2`, `completed=false`, `extraction_run_id=0279a558-c83e-428d-a564-a345749dd635`
  - `metro_abarrotes.json`: `next_offset=40`, `pages_completed=2`, `completed=false`, `extraction_run_id=0279a558-c83e-428d-a564-a345749dd635`
  - `plazavea_lacteos.json`: `next_offset=33`, `pages_completed=2`, `completed=true`, `extraction_run_id=b374f6cf-1762-4c6f-9dae-f7998fb1f2eb` *(categoría agotada en pág. 2 con 13 productos < `page_size=20`)*
  - `plazavea_abarrotes.json`: `next_offset=40`, `pages_completed=2`, `completed=false`, `extraction_run_id=b374f6cf-1762-4c6f-9dae-f7998fb1f2eb`
  - `tottus_leche.json`: `next_offset=48`, `pages_completed=1`, `completed=false`, `extraction_run_id=23fb4813-c9cb-4ae9-87ba-fc41a03498a3` *(piloto ejecutado con `--page-size 48 --max-pages 1`)*
  - `tottus_abarrotes.json`: `next_offset=48`, `pages_completed=1`, `completed=false`, `extraction_run_id=23fb4813-c9cb-4ae9-87ba-fc41a03498a3`

---

## 3. Auditoría Layer Bronze (DuckDB)

Salida emitida por `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze`:

## Bronze Layer Audit Report

- **Bronze Dir:** `data/bronze`
- **Parquet Files Scanned:** `3`

### Coverage, Deduplication & Stock Summary

| Scope | Rows | Products | SKUs | SKU/Prod | EAN Valid (%) | Dup Flag (%) | Hash Dups (%) | Null `raw_payload` (%) | In Stock (%) | Card < Selling (%) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | 81 | 80 | 81 | 1.01 | 100.00% | 0.00% | 1.23% | 0.00% | 98.77% | 0.00% |
| plazavea | 73 | 73 | 73 | 1.00 | 78.08% | 0.00% | 0.00% | 0.00% | 94.52% | 1.37% |
| tottus | 96 | 96 | 96 | 1.00 | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% | 1.04% |
| **TOTAL** | 250 | 249 | 250 | 1.00 | 55.20% | 0.00% | 0.40% | 0.00% | 59.60% | 0.80% |

### Price Distribution (PEN)

| Scope | Metric | Non-Null | Min | P25 | P50 | Mean | P75 | Max |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | `list_price` | 80 | 1.90 | 5.90 | 11.95 | 14.95 | 23.60 | 80.40 |
| metro | `selling_price` | 80 | 1.00 | 4.97 | 9.70 | 13.13 | 20.05 | 80.40 |
| metro | `card_price` | 0 | N/A | N/A | N/A | N/A | N/A | N/A |
| plazavea | `list_price` | 72 | 1.60 | 4.47 | 6.50 | 13.37 | 18.77 | 78.50 |
| plazavea | `selling_price` | 72 | 1.60 | 4.30 | 5.80 | 12.92 | 18.05 | 78.50 |
| plazavea | `card_price` | 1 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 |
| tottus | `list_price` | 50 | 4.80 | 10.85 | 18.55 | 19.15 | 24.90 | 49.20 |
| tottus | `selling_price` | 96 | 2.40 | 6.10 | 13.40 | 18.08 | 21.60 | 309.00 |
| tottus | `card_price` | 1 | 20.50 | 20.50 | 20.50 | 20.50 | 20.50 | 20.50 |
| **TOTAL** | `list_price` | 202 | 1.60 | 5.70 | 12.95 | 15.43 | 23.80 | 80.40 |
| **TOTAL** | `selling_price` | 248 | 1.00 | 4.90 | 9.90 | 14.98 | 20.75 | 309.00 |
| **TOTAL** | `card_price` | 2 | 4.90 | 8.80 | 12.70 | 12.70 | 16.60 | 20.50 |

### Notas de Auditoría de Datos Bronze

1. **Separación real 1:N (`metro`):** El producto `product_id = "1021580"` (*Gelatina Umsha Sabor Fresa 130g*) expandió en 2 SKUs (`39260526` con stock `99999` a `S/ 3.80` y `39338936` sin stock `0` y precio `None`), explicando `80` productos vs `81` SKUs (`SKU/Prod = 1.0125`) y `Hash Dups = 1.23%` aun cuando `Dup Flag = 0.00%` (ambas filas comparten el mismo `payload_hash` del producto padre).
2. **Cobertura EAN y Stock en Tottus (`0.00%`):** La inspección directa del `raw_payload` de `tottus` confirma que los items de `__NEXT_DATA__` (`props.pageProps.results`) no incluyen `ean` ni stock numérico (`availability` solo expone banderas vacías de despacho). La arquitectura delega Tottus a enriquecimiento/matching en E3.
3. **Outlier de precio máximo en Tottus (`S/ 309.00`):** Corresponde al SKU `117905576` (*Olla Arrocera Multiusos de 1.8L Modelo ARR-AGH01*), retornado por búsqueda de texto `?Ntt=abarrotes&page=1` en Tottus (46 de 96 items en Tottus no trajeron `normalPrice` -> `list_price = NULL`, usando `internetPrice` en `selling_price`).

---

## 4. Matriz de Cumplimiento BMAD (E1-01 a E1-04)

| Story / Spec | Criterio de Aceptación | Estado | Evidencia en Código / Artefactos |
| :--- | :--- | :---: | :--- |
| **E1-01.1** | Modelo 1:N `Product` -> `Vec<SkuItem>` y `Product::to_bronze_rows(is_duplicate_payload)` conservando `product_id`/`payload_hash` y omitiendo `raw_payload` en duplicados | **PASS** | `rust-ingestor/crates/ingestor/src/domain.rs:5-139` + 4 tests unitarios. |
| **E1-01.2** | Trait `RetailerAdapter` (`retailer_slug`, `extractor_version`, `build_page_url`, `parse_page_payload`, `fetch_page`) | **PASS** | `rust-ingestor/crates/ingestor/src/adapter.rs:57-70` + `FetchContext` y tests de object-safety. |
| **E1-01.3** | `compute_payload_hash` (SHA-256 hex) y `PayloadDeduplicator` persistente en JSON | **PASS** | `rust-ingestor/crates/ingestor/src/dedup.rs:10-67` (`seen_hashes.json` con escritura atómica). |
| **E1-01.4** | `CheckpointStore` persistiendo `next_offset`, `pages_completed`, `extraction_run_id`, `updated_at`, `completed` por `(retailer, category)` | **PASS** | `rust-ingestor/crates/ingestor/src/checkpoint.rs:9-83` + commit post-Parquet en `runner.rs:159-161`. |
| **E1-02.1** | `VtexAdapter` (`plazavea`, `metro`, `wong`), paginación `_from`/`_to`, multi-SKU, `ean` vacío -> `None`, `card_price` por teasers, `governor <= 2 req/s`, backoff máx. 3 reintentos | **PASS** | `rust-ingestor/crates/ingestor/src/adapters/vtex.rs:13-375` y `http.rs:34-193`. |
| **E1-02.2** | `FalabellaListingAdapter` (`tottus`), JSON directo + `<script id="__NEXT_DATA__">`, mapeo `normalPrice`/`internetPrice`/`eventPrice`/`cmrPrice`, rate limit + backoff | **PASS** | `rust-ingestor/crates/ingestor/src/adapters/falabella.rs:15-306`. |
| **E1-03.1** | `write_bronze_parquet` con ruta `dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet`, compresión `ZSTD(3)`, esquema estricto de 28 columnas y preservación de nulos | **PASS** | `rust-ingestor/crates/ingestor/src/storage/parquet_writer.rs:19-186` (`into_inner()?.sync_all()?` + rename atómico). |
| **E1-03.2** | CLI `clap` con los 8 flags requeridos, `tracing` JSON y emisión `metrics-<run_id>.json` con métricas operativas y trazabilidad de estado | **PASS** | `rust-ingestor/crates/ingestor/src/main.rs:43-111`, `metrics.rs:8-79`, `runner.rs:163-222`. |
| **E1-04.1** | DDL `sql/postgres_schema.sql` con `extraction_runs` + 5 tablas maestras e índices SCD Tipo 2 | **PASS** | `sql/postgres_schema.sql:1-162` (alineado con `started_at`, `finished_at`, `status` de `RunMetrics`). |
| **E1-04.2** | Script DuckDB `scripts/inspect_bronze.py` + `tests/test_inspect_bronze.py` auditando cobertura EAN, SKU/Prod, duplicados, stock y percentiles de precios | **PASS** | `scripts/inspect_bronze.py:1-356` + `tests/test_inspect_bronze.py:1-530` (`8/8` pytest PASS). |
| **Alcance E1** | Sin reglas completas de normalización E2 (unidades `g`/`kg`/`ml`, `unit_price_std`) ni embeddings/matching E3 | **PASS** | Verificado en todos los módulos: únicamente limpieza de strings vacíos (`normalize_opt_string`) y extracción cruda Bronze. |

---

## 5. Hallazgos HTTP, Esquemas y Bugs Detectados / Resueltos en Piloto

1. **VTEX HTTP `206 Partial Content` vs `200 OK`:**
   - La API VTEX `/api/catalog_system/pub/products/search/...?_from=X&_to=Y` responde **`206 Partial Content`** cuando hay más resultados en la categoría (`81/81` filas de Metro y `60/73` filas de Plaza Vea) y **`200 OK`** en la última página parcial (`13/73` filas de `plazavea_lacteos`). `RateLimitedHttpClient` (`http.rs:93`) valida `resp.status().is_success()`, aceptando ambos códigos sin fallar ni reintentar (`http_client_accepts_206_partial_content`).
2. **Tottus (`__NEXT_DATA__`) sin bloqueo WAF Cloudflare en el piloto:**
   - Las 2 peticiones a `https://tottus.falabella.com.pe/tottus-pe/search?Ntt=...&page=1` devolvieron HTTP `200 OK` (`4.44 MB` HTML acumulado) con `<script id="__NEXT_DATA__" type="application/json">` parseado limpiamente en `48` productos/página (`96` total), `0` errores HTTP. En caso de bloqueo WAF (HTTP 403 o challenge HTML sin `__NEXT_DATA__`), `http.rs:141-152` corta sin reintentos inútiles y `falabella.rs:49-53` emite error descriptivo con estado HTTP y snippet sin panic.
3. **Bug `PromotionalPriceTableItemsDiscount` en `vtex.rs` (detectado en piloto y protegido con TDD):**
   - En Plaza Vea (`sku=10672464`, *Filete de Atún CAMPOMAR 150g*, `ListPrice: 5.90`, `Price: 5.20`), los `PromotionTeasers` (*TARJETA OH - PVEA*, `PaymentMethodId: 210`) reportan `{"Name": "PromotionalPriceTableItemsDiscount", "Value": "1.00"}`. El descuento VTEX aplica sobre `ListPrice` (`5.90 - 1.00 = 4.90`) y **no** sobre `Price` (`5.20 - 1.00 = 4.20`). Protegido por `vtex_extracts_card_price_from_price_table_discount_and_max_unit_price_ignoring_shipping` (`vtex.rs:555-619`).
4. **Precios `0.0` en VTEX sin stock normalizados a `None`:**
   - En Plaza Vea (`sku=12399456`) y Metro (`sku=39338936`), VTEX devolvió `ListPrice: 0.0, Price: 0.0, AvailableQuantity: 0`. `positive_f64` (`vtex.rs:62-69`) normalizó ambos precios a `None` en Parquet, evitando confundir quiebre de stock con precio cero.

---

## 6. Estado de Hallazgos de Revisión de Código (`Critical` / `Important` / `Minor`)

### Critical
*Ninguno encontrado.*

### Important (Todos Resueltos y Verificados)

- **[IMP-01] `RESUELTO` — Orden de persistencia (`dedup.persist()` y `checkpoints.save()`) posterior a `write_bronze_parquet`**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/runner.rs:49, 110-141, 151-161`. Los checkpoints de cada categoría se acumulan en `pending_checkpoints` y solo se escriben en disco junto a `dedup.persist()?` **después** de que `write_bronze_parquet(...)` finaliza y renombra atómicamente el Parquet. Verificado por el test TDD `run_ingest_does_not_persist_dedup_or_checkpoints_if_parquet_write_fails` (`runner.rs:456-492`).

- **[IMP-02] `RESUELTO` — Manejo explícito de fallo parcial en corridas multi-categoría**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/runner.rs:164-220`. Si alguna categoría falla tras haber recuperado páginas previas, `run_ingest` persiste el Parquet parcial, los hashes deduplicados, los checkpoints completados y `metrics-<run_id>.json` con `status: "partial_error"` y `error: Some(...)`, y retorna `Err(err)` (exit code != 0). Verificado por el test TDD `run_ingest_returns_err_on_partial_category_failure_while_persisting_succeeded_work` (`runner.rs:422-453`).

### Minor (Seguimiento y Estado)

- **[MIN-02] `RESUELTO (Parcial/Acotado a E1)` — Validación de `MinimumQuantity`, `PromotionalPriceTableItemsIds` y simplificación en `extract_card_price`**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/adapters/vtex.rs:84-127, 157`. Se filtran teasers con `MinimumQuantity > 1` y se valida que `PromotionalPriceTableItemsIds` incluya al `sku_id` actual (`vtex_ignores_teaser_when_sku_id_not_targeted_or_min_qty_gt_1`, L622-665); `val.round() / 100.0` simplificado en L157. *(Nota no bloqueante: `Conditions.Parameters[].PaymentMethodId` no se filtra por lista fija en E1 ya que los IDs varían por tienda VTEX y la elegibilidad promocional completa corresponde a E2).*
- **[MIN-03] `RESUELTO` — Clamp de `page_size` a `[1, 50]` en `VtexAdapter`**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/adapters/vtex.rs:250-251, 358-364`. `build_page_url` y `fetch_page` aplican `page_size.clamp(1, 50)`, verificado por `vtex_clamps_page_size_to_50_to_match_api_limit` (L668-702).
- **[MIN-04] `RESUELTO (Durabilidad)` / `ABIERTO (Streaming)` — `sync_all()` en `parquet_writer.rs` vs buffer en RAM**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/storage/parquet_writer.rs:170-171` ahora ejecuta `let inner = writer.into_inner()?; inner.sync_all()?;` antes de `fs::rename`. *(Nota no bloqueante para escalamiento futuro: `all_rows` en `runner.rs:48` sigue reteniendo filas en RAM hasta el cierre de la corrida; `peak_ram_mb` en piloto fue `<= 28.57 MB`).*
- **[MIN-05] `RESUELTO` — Campos `started_at`, `finished_at`, `status`, `error` en `RunMetrics` y validación CLI**
  - **Evidencia:** `rust-ingestor/crates/ingestor/src/metrics.rs:14-17` añade los campos alineados con `sql/postgres_schema.sql:10-28`; `main.rs:93-99` rechaza `--max-pages 0` y `--market-id` en blanco, cubierto por `cli_rejects_zero_max_pages_and_blank_market_id` (L207-231).
- **[MIN-09] `RESUELTO (Cargo.toml & fmt)` / `ABIERTO (slug)` — Eliminación de `thiserror` y `cargo fmt` limpio**
  - **Evidencia:** `rust-ingestor/crates/ingestor/Cargo.toml` removió `thiserror`; `cargo fmt --check` pasa con `exit 0`. *(Permanece como observación menor la colisión potencial en `checkpoint::sanitize_category` L25-48 si dos categorías difieren únicamente en caracteres no alfanuméricos, ej. `"leche/entera"` vs `"leche entera"`).*
- **[MIN-01] `ABIERTO (No bloqueante)` — Acoplamiento `offset / page_size` en `FalabellaListingAdapter`**
  - **Archivo y líneas:** `rust-ingestor/crates/ingestor/src/adapters/falabella.rs:247, 260-262, 300`, `checkpoint.rs:10-18`
  - **Detalle:** `build_page_url` calcula `page = (offset / page_size.max(1)) + 1` y `next_offset = offset + page_size.max(1)` aunque Tottus devuelve 48 items/página fijos (`CheckpointState` no persiste `page_size`, por lo que cambiar `--page-size` durante `--resume` desalinearía la página; con `--page-size` consistente funciona correctamente).
- **[MIN-06] `ABIERTO (No bloqueante)` — Medición de `mb_transferred` sobre cuerpo UTF-8 descomprimido**
  - **Archivo y líneas:** `rust-ingestor/crates/ingestor/src/http.rs:96`, `Cargo.toml:9`
  - **Detalle:** `body.len()` contabiliza bytes de texto UTF-8 tras descompresión `gzip`/`brotli` automática de `reqwest`, no bytes comprimidos sobre red.
- **[MIN-07] `ABIERTO (No bloqueante)` — Etiqueta `Hash Dups (%)` en `inspect_bronze.py` incluye abanico 1:N multi-SKU**
  - **Archivo y líneas:** `scripts/inspect_bronze.py:100, 131`, `tests/test_inspect_bronze.py:322-324`
  - **Detalle:** `hash_duplicates_count = row_count - distinct_payload_hashes` contabiliza filas SKU que comparten el `payload_hash` del producto padre (`Metro` `1.23%` con `Dup Flag = 0.00%`). Comportamiento probado en `pytest`; conviene mantener explícita su diferencia frente a `Dup Flag (%)`.
- **[MIN-08] `ABIERTO (No bloqueante)` — Índice parcialmente redundante en `dim_trade_item.gtin` y unicidad SCD2**
  - **Archivo y líneas:** `sql/postgres_schema.sql:67, 80-82, 128-130`
  - **Detalle:** `gtin VARCHAR(14) UNIQUE` crea implícitamente un índice B-tree único completo, haciendo parcialmente redundante a `idx_dim_trade_item_gtin ... WHERE gtin IS NOT NULL`; `uq_bridge_sku_trade_item_active` garantiza unicidad activa (`valid_to IS NULL`) sin requerir extensión `btree_gist` en E1.
