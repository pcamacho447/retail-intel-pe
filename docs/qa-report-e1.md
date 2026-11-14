# Reporte QA y Auditoría Benchmark — Fase E1 / E1.5 Hardening (`retail-intel-pe`)

- **Fecha de auditoría:** 2026-11-14 (cierre Fase **E1.5 Hardening**, versión `0.1.5` sobre corte piloto `dt=2026-11-14`)
- **Roles BMAD / Subagentes:** `QABenchmarkAgent` (`v0.1.0`) + `VtexHardeningAgent`, `TottusHardeningAgent`, `QAHardeningAgent` (`v0.1.5`)
- **Metodología aplicada:** `systematic-debugging` + TDD estricto + `verification-before-completion` contra `docs/architecture-e1.md` (`v0.1.5`) y `docs/stories/e1.5-hardening-bronze.md`
- **Dictamen QA:** **`APPROVED / SHIP (E1.5 Complete — Ready for E2 Silver)`** *(0 Critical · 0 Important abiertos · Todos los Minor `MIN-01` a `MIN-09` cerrados)*

> **Resumen ejecutivo del dictamen:** El hardening **E1.5 (`v0.1.5`)** completa los **4 retailers objetivo** (`plazavea`, `metro`, `wong`, `tottus`) sobre un esquema Bronze ZSTD de **29 columnas** (añadiendo `card_program`), cierra las observaciones Minor pendientes (`MIN-01`, `MIN-06`, `MIN-07`, `MIN-08` y colisión de slugs en `MIN-09`), eleva la cobertura de EAN en Plaza Vea (`78.08% -> 80.82%` no nulos; `71.23% -> 73.97%` GS1 Mod-10 válido vía fallback `referenceId`), y habilita en Tottus disponibilidad real (`100.00%` en stock, vs `0.00%` en `v0.1.0`), tarjeta `CMR` y cobertura EAN verificada (`81.25%`) mediante enriquecimiento PDP selectivo y caché persistente en `state/tottus_ean_cache.json`. Todas las suites pasan con **0 errores ni advertencias**: **63/63** tests Rust (`58` lib + `5` bin), **13/13** tests Python/DuckDB, `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, y **26/26** requests HTTP en vivo con **0 errores**.

---

## 1. Resumen de Suites y Controles Verificados (`v0.1.5`)

| Verificación | Comando Ejecutado / Alcance | Resultado | Evidencia |
| :--- | :--- | :--- | :--- |
| **Rust Formatter (`rustfmt`)** | `cd rust-ingestor && cargo fmt --check` | **PASS (`exit 0`)** | 0 diferencias de formato en todo el workspace Cargo. |
| **Rust Linter (`clippy`)** | `cd rust-ingestor && cargo clippy --all-targets -- -D warnings` | **PASS (`0` warnings)** | Compilación limpia sin advertencias en lib, bin y tests. |
| **Rust Unit & Integration Tests** | `cd rust-ingestor && cargo test` | **PASS (`63/63`)** | `58 passed` en `src/lib.rs` + `5 passed` en `src/main.rs`; `0 failed`, `0 ignored`. |
| **Release Binary** | `cd rust-ingestor && cargo build --release` | **PASS (`exit 0`)** | `rust-ingestor/target/release/ingestor` compilado en perfil `release` (`4.03s`). |
| **Python DuckDB Audit Suite** | `.venv/bin/pytest -v` | **PASS (`13/13`)** | `13 passed in 1.06s` en `tests/test_inspect_bronze.py`. |
| **DuckDB Bronze Audit (Piloto 4 Retailers)** | `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze` | **PASS (`exit 0`)** | 4 particiones Parquet escaneadas (`361` filas, `359` productos únicos, `360` SKUs únicos), `ZSTD`, esquema de **29 columnas**. |
| **PostgreSQL DDL (`sql/postgres_schema.sql`)** | Inspección estática y limpieza de índices redundantes (`MIN-08`) | **VERIFICADO (Estático)** | Removido índice redundante `idx_dim_trade_item_gtin` al quedar cubierto por `gtin VARCHAR(14) UNIQUE`. |

---

## 2. Telemetría Operativa del Piloto E1.5 (`dt=2026-11-14`, 4 Retailers)

Datos consolidados desde los `metrics-*.json` emitidos por `rust-ingestor/target/release/ingestor` y contrastados contra `data/bronze/checkpoints/*.json`, `data/bronze/blobs/seen_hashes.json` y los archivos Parquet ZSTD reales:

| retailer | Categorías piloto | extraction_run_id | status | products_recovered | skus_recovered | pages_fetched | requests_ok | requests_error | avg_latency_ms | mb_transferred | total_duration_secs | peak_ram_mb |
| :--- | :--- | :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `plazavea` | `lacteos,abarrotes` | `622e4ed8-8214-402f-aa54-dd5910a2fe92` | `success` | 73 | 73 | 4 | 4 | 0 | 248.94 | 0.9232 | 1.70 | 18.41 |
| `metro` | `lacteos,abarrotes,televisor` | `019e8816-8a80-4db0-9a17-c404c56ce89d` | `success` | 119 | 120 | 6 | 6 | 0 | 317.15 | 5.1091 | 2.93 | 36.88 |
| `wong` | `lacteos,abarrotes,televisor` | `2bbed44e-10b4-403a-b380-b5b561991b55` | `success` | 120 | 120 | 6 | 6 | 0 | 423.53 | 5.5453 | 3.46 | 40.99 |
| `tottus` | `leche` (`--enrich-tottus-pdp`) | `6f072907-0f6f-4253-98f7-f1aba05f6aef` | `success` | 48 | 48 | 1 | 10 | 0 | 2042.24 | 12.6477 | 24.05 | 20.71 |
| **TOTAL / AGG** | **4 retailers** | `4 corridas` | **4/4 `success`** | **360** *(359 únicos)* | **361** *(360 únicos)* | **17** | **26** *(17 pág + 9 PDP)* | **0** | **—** | **24.2253** | **—** | **máx. 40.99** |

### Consistencia Artefactos vs. Métricas, Blobs y Checkpoints

- **Parquet vs. `metrics-*.json`:** Exactitud `100%` en los 4 retailers (`plazavea`: 73 filas, `150.3 KiB` en disco / `1,032,646` bytes sin comprimir; `metro`: 120 filas, `521.7 KiB` / `5,550,307` bytes; `wong`: 120 filas, `465.4 KiB` / `5,967,837` bytes; `tottus`: 48 filas, `25.2 KiB` / `83,278` bytes), todos con `ZSTD`, `1` row group y **29 columnas**.
- **Blob Store SHA-256 (`data/bronze/blobs/seen_hashes.json`):** Contiene **`360`** hashes SHA-256 distintos bajo `seen_hashes`, con igualdad exacta (`360 == DISTINCT payload_hash` en Parquet across all 4 files).
- **Caché Tottus (`state/tottus_ean_cache.json`):** `52` entradas persistidas tras la corrida (`43` GTIN-13 pre-sembrados por cruce VTEX con `pdp_checked: false` + `9` SKUs sin caché consultados en vivo contra su PDP con `pdp_checked: true`, `source: "tottus_pdp_ssr"`, `in_stock: true`, `available_qty: 24 | 10` y `seller_sku_id`). En futuras corridas esos 52 SKUs evitan peticiones PDP redundantes.
- **Checkpoints (`data/bronze/checkpoints/*.json`):** Los 9 archivos (`metro_*`, `plazavea_*`, `wong_*`, `tottus_leche`) registran explícitamente `"page_size"` (`20` para VTEX, `48` para Tottus) conforme a `MIN-01`. En Wong, el producto `1110400` / SKU `39345952` apareció simultáneamente en `lacteos` y `abarrotes` (2 filas con distinto payload de categoría/posición). En Metro, el producto con expansión `1:N` generó 2 SKUs compartiendo el mismo `payload_hash` (`Hash Dups = 0.83%`, `1:N Fanout = 0.83%`).

---

## 3. Auditoría Layer Bronze (DuckDB `scripts/inspect_bronze.py`)

Salida emitida por `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze`:

### Coverage, Deduplication & Stock Summary

| Scope | Rows | Products | SKUs | SKU/Prod | EAN Valid (%) | EAN Mod-10 (%) | Dup Flag (%) | Hash Dups (%) | 1:N Fanout (%) | Null `raw_payload` (%) | In Stock (%) | Card < Selling (%) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | 120 | 119 | 120 | 1.01 | 100.00% | 100.00% | 0.00% | 0.83% | 0.83% | 0.00% | 99.17% | 31.67% |
| plazavea | 73 | 73 | 73 | 1.00 | 80.82% | 73.97% | 0.00% | 0.00% | 0.00% | 0.00% | 94.52% | 1.37% |
| tottus | 48 | 48 | 48 | 1.00 | 81.25% | 81.25% | 0.00% | 0.00% | 0.00% | 0.00% | 100.00% | 2.08% |
| wong | 120 | 119 | 119 | 1.00 | 100.00% | 100.00% | 0.00% | 0.00% | 0.83% | 0.00% | 100.00% | 24.17% |
| **TOTAL** | 361 | 359 | 360 | 1.00 | 93.63% | 92.24% | 0.00% | 0.28% | 0.55% | 0.00% | 98.61% | 19.11% |

### Card Programs Breakdown

| Scope | Program | Active Offers |
| :--- | :--- | ---: |
| metro | Cencosud | 38 |
| plazavea | Oh! | 1 |
| tottus | CMR | 1 |
| wong | Cencosud | 29 |
| **TOTAL** | Cencosud | 67 |
| **TOTAL** | CMR | 1 |
| **TOTAL** | Oh! | 1 |

### Price Distribution (PEN)

| Scope | Metric | Non-Null | Min | P25 | P50 | Mean | P75 | Max |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | `list_price` | 119 | 1.90 | 6.90 | 22.90 | 891.86 | 1649.00 | 6999.00 |
| metro | `selling_price` | 119 | 1.00 | 6.60 | 19.90 | 691.32 | 1249.00 | 5899.00 |
| metro | `card_price` | 38 | 399.00 | 1219.00 | 1674.00 | 2016.37 | 2549.00 | 5699.00 |
| plazavea | `list_price` | 72 | 1.60 | 4.47 | 6.50 | 13.37 | 18.77 | 78.50 |
| plazavea | `selling_price` | 72 | 1.60 | 4.30 | 5.80 | 12.92 | 18.05 | 78.50 |
| plazavea | `card_price` | 1 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 |
| tottus | `list_price` | 32 | 5.20 | 12.97 | 18.90 | 19.11 | 25.12 | 49.20 |
| tottus | `selling_price` | 48 | 2.50 | 5.15 | 13.40 | 13.74 | 21.50 | 44.90 |
| tottus | `card_price` | 1 | 20.50 | 20.50 | 20.50 | 20.50 | 20.50 | 20.50 |
| wong | `list_price` | 120 | 1.90 | 6.58 | 21.75 | 882.05 | 1524.00 | 6999.00 |
| wong | `selling_price` | 120 | 1.80 | 5.88 | 19.90 | 658.39 | 1161.50 | 5899.00 |
| wong | `card_price` | 29 | 399.00 | 1099.00 | 1449.00 | 2017.28 | 2999.00 | 5699.00 |
| **TOTAL** | `list_price` | 343 | 1.60 | 6.15 | 18.50 | 622.60 | 38.00 | 6999.00 |
| **TOTAL** | `selling_price` | 359 | 1.00 | 5.20 | 16.30 | 453.66 | 33.25 | 5899.00 |
| **TOTAL** | `card_price` | 69 | 4.90 | 1099.00 | 1499.00 | 1958.67 | 2599.00 | 5699.00 |

---

## 4. Muestra Comparativa Cross-Retailer Verificada (Bronze `dt=2026-11-14`)

Ejemplos reales extraídos con DuckDB uniendo los 4 retailers mediante EAN/GTIN validado y ofertas con programa de tarjeta:

| Producto Comparado | Retailer | SKU | P. Lista | P. Venta | P. Tarjeta + Programa | EAN | Stock |
| :--- | :--- | :--- | ---: | ---: | :--- | :--- | ---: |
| **Arroz Extra Añejo Faraón Naranja 5kg** | `metro` | `34015` | 26.90 | 21.90 | N/A | `7754294000095` | 99999 |
| **Arroz Extra Añejo Faraón Naranja 5kg** | `plazavea` | `28250` | 26.90 | 21.90 | N/A | `7754294000095` | 578 |
| **Arroz Extra Añejo Faraón Naranja 5kg** | `wong` | `34015` | 26.90 | 24.90 | N/A | `7754294000095` | 99999 |
| **Leche Reconstituida Entera Gloria Sixpack 390g** | `tottus` | `129087925` | 24.60 | 21.50 | **20.50 (CMR)** | `7751271034098` | 1 |
| **Leche Reconstituida Entera Gloria Sixpack 390g** | `wong` | `39214340` | 24.60 | 23.30 | N/A | `7751271034098` | 99999 |
| **Mezcla Láctea IDEAL Cremosita Sixpack 390g** | `plazavea` | `10670704` | 23.50 | 21.90 | N/A | `8445290404787` | 92 |
| **Mezcla Láctea IDEAL Cremosita Sixpack 390g** | `tottus` | `113706451` | 24.60 | 21.90 | N/A | `8445290404787` | 1 |
| **Filete de Atún CAMPOMAR en Aceite Vegetal 150g** | `plazavea` | `10672464` | 5.90 | 5.20 | **4.90 (Oh!)** | `7750408003235` | 2154 |
| **Televisor Smart TV TCL 55" QDMINILED 55C6K** | `metro` | `39308719` | 2149.00 | 1899.00 | **1799.00 (Cencosud)** | `6921732845582` | 99999 |
| **Televisor Smart TV TCL 55" QDMINILED 55C6K** | `wong` | `39312006` | 2299.00 | 1849.00 | **1799.00 (Cencosud)** | `6921732845582` | 99999 |
| **Margarina LAIVE Swis Barra 90g** *(fallback refId)* | `plazavea` | `54187` | 3.00 | 3.00 | N/A | `20181512` *(EAN-8 Mod-10)* | 224 |
| **Galletas VICTORIA Margarita 6un** *(fallback refId)* | `plazavea` | `12274833` | 5.40 | 4.70 | N/A | `20570316` *(EAN-8 Mod-10)* | 307 |

---

## 5. Análisis Técnico de Brechas Investigadas en E1.5 (`systematic-debugging`)

1. **Tottus `__NEXT_DATA__` vs. PDP HTML y Arquitectura de Caché (`state/tottus_ean_cache.json`):**
   - El listado `__NEXT_DATA__` de Tottus (`props.pageProps.results`) omite `ean`/`gtin13` y no reporta cantidades numéricas de stock (`availability` trae strings vacíos), por lo que en `v0.1.0` figuraba con `0.00%` en ambas métricas.
   - Al inspeccionar PDPs reales de Tottus en vivo (`--enrich-tottus-pdp`), el SSR expone `<script type="application/ld+json">` (`offers[].availability = "https://schema.org/InStock"`) y `__NEXT_DATA__.props.pageProps.productData` (`isOutOfStock: false`, `variants[].maxOnCartAmount` = `10` o `24`, `sellerSkuId`), pero en el PIM de Falabella varios SKUs de supermercado emiten `"gtin13": "null"`. Por ello `falabella.rs` combina: (a) extracción en listado de señales activas/numéricas, (b) enriquecimiento PDP limitado a SKUs sin caché previa respetando `governor <= 2 req/s` (capturando `InStock`, `maxOnCartAmount` y `gtin13` cuando existe), y (c) caché persistente `state/tottus_ean_cache.json` con `43` GTIN-13 verificados Mod-10 pre-sembrados (que nunca se sobrescriben si la PDP devuelve `"null"`), logrando **81.25%** EAN Mod-10 y **100.00%** cobertura de disponibilidad.
2. **Plaza Vea `items[].ean` vacío y códigos internos (`78.08% -> 80.82%` non-empty / `71.23% -> 73.97%` Mod-10):**
   - De los 16 SKUs originalmente sin EAN en Plaza Vea, `extract_vtex_ean` inspeccionó `items[].referenceId[].Value`, `productReference`, `productReferenceCode` y especificaciones con validación GS1 Mod-10 (`validate_gtin_mod10`), recuperando 2 EAN-8 válidos adicionales (`sku=54187` -> `20181512`, `sku=12274833` -> `20570316`), mientras otros 14 items solo poseen códigos internos no-GS1 (`VTCPERU-...`, `926320`, etc.) y quedaron en `NULL`. Adicionalmente, `5` ítems (`137077857`, `139419365`, `151258566`, `151261073`, `2026041409335179`) traían valores propios no-GS1 ya presentes en `items[].ean` (preservados en Bronze como campo crudo, y filtrados explícitamente por la métrica `EAN Mod-10 (%)` en DuckDB).
3. **Visibilidad de `card_price` y `card_program` en la API pública VTEX:**
   - En abarrotes/lácteos de Metro y Wong la gran mayoría de teasers públicos en búsqueda corresponden a promociones por volumen (`MinimumQuantity >= 2`, p. ej. `Tag-p3x11.50`) o despacho gratis (`ShippingPercentualDiscount`), no a precio unitario con tarjeta. Cuando existen descuentos unitarios con tarjeta (p. ej. `TARJETA OH - PVEA` en Plaza Vea o `[TCENCO] ... Con TC Metro / Wong` en electrohogar/televisores, además de teasers con claves C# `<Field>k__BackingField`), `extract_card_offer` extrae tanto `card_price` como `card_program` (`Oh!`, `Cencosud`, `Wong Prime`), validado aquí con `38` ofertas `Cencosud` en Metro, `29` en Wong, `1` `Oh!` en Plaza Vea y `1` `CMR` en Tottus.

---

## 6. Estado Final de Hallazgos de Revisión (`Critical` / `Important` / `Minor`)

| ID | Severidad | Estado E1.5 | Evidencia de Cierre |
| :--- | :--- | :---: | :--- |
| **IMP-01** | Important | **CERRADO (`v0.1.0`)** | `runner.rs`: `dedup.persist()` y `checkpoints.save()` solo se ejecutan tras `write_bronze_parquet` (`run_ingest_does_not_persist_dedup_or_checkpoints_if_parquet_write_fails`). |
| **IMP-02** | Important | **CERRADO (`v0.1.0`)** | `runner.rs`: persistencia de avance parcial + `status: "partial_error"` y retorno `Err(...)` (`run_ingest_returns_err_on_partial_category_failure_while_persisting_succeeded_work`). |
| **MIN-01** | Minor | **CERRADO (`v0.1.5`)** | `checkpoint.rs` & `runner.rs`: `CheckpointState` añade `#[serde(default)] pub page_size: Option<usize>`; `--resume` adopta `cp.page_size` cuando existe (`run_ingest_resume_uses_saved_checkpoint_page_size_when_cli_differs` y `checkpoint_state_defaults_missing_page_size_to_none`). |
| **MIN-02** | Minor | **CERRADO (`v0.1.0`)** | `vtex.rs`: filtra `MinimumQuantity > 1` y verifica `PromotionalPriceTableItemsIds` por SKU (`vtex_ignores_teaser_when_sku_id_not_targeted_or_min_qty_gt_1`). |
| **MIN-03** | Minor | **CERRADO (`v0.1.0`)** | `vtex.rs`: clamp `page_size.clamp(1, 50)` en `build_page_url` y `fetch_page` (`vtex_clamps_page_size_to_50_to_match_api_limit`). |
| **MIN-04** | Minor | **CERRADO (`v0.1.0`)** | `parquet_writer.rs`: `writer.into_inner()?.sync_all()?;` antes de `fs::rename` atómico. *(Nota de diseño para E2/E3: buffering por lote sigue en `<= 40.99 MB` pico en piloto).* |
| **MIN-05** | Minor | **CERRADO (`v0.1.0`)** | `metrics.rs` & `main.rs`: añade `started_at`, `finished_at`, `status`, `error`; CLI rechaza `--max-pages 0` y `--market-id` vacío. |
| **MIN-06** | Minor | **CERRADO (`v0.1.5`)** | `http.rs`: mide `resp.content_length()` como bytes reales transferidos con fallback a `raw_bytes.len()` previo a decodificación UTF-8 (`http_client_reports_wire_content_length_and_chunked_fallback`). |
| **MIN-07** | Minor | **CERRADO (`v0.1.5`)** | `scripts/inspect_bronze.py` & `tests/test_inspect_bronze.py`: separa explícitamente `Hash Dups (%)` de `1:N Fanout (%)` (`multi_sku_fanout_count`/`pct`), agrega `EAN Mod-10 (%)`, `card_program_counts`, soporte mixto 28/29 columnas y tabla de muestras multi-retailer (`13/13` tests pytest PASS). |
| **MIN-08** | Minor | **CERRADO (`v0.1.5`)** | `sql/postgres_schema.sql`: removido índice redundante `idx_dim_trade_item_gtin` sobre `dim_trade_item.gtin`, documentando el índice B-tree único implícito de `gtin VARCHAR(14) UNIQUE`. |
| **MIN-09** | Minor | **CERRADO (`v0.1.5`)** | `checkpoint.rs`: `CheckpointStore::hashed_path_for` y `resolve_path_for_category` evitan colisiones de slug (`checkpoint_store_disambiguates_colliding_category_slugs`). |
