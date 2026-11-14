# Reporte QA y Auditoría de Contrato Bronze v2 — Fase E1.6 (`retail-intel-pe`)

- **Fecha de auditoría:** 2026-11-14 (partición de negocio Lima `dt=2026-11-14`, rama `feat/e1.6-bronze-contract`, binario compilado en `098958b`)
- **Contrato verificado:** `bronze_contract_version = "2.0.0"` (`docs/bronze-contract-v2.md`, esquema Parquet ZSTD de **40 columnas**)
- **Política de rastreo asociada:** `docs/crawl-policy.md` (presupuesto piloto acotado `<= 65` requests HTTP a `<= 2 req/s`)
- **Dictamen QA (Pasos 1–6):** **`PASS WITH GATE (--allow-truncated) — Pending Day-2 Repeatability & Branch Review`**

> **Resumen ejecutivo:** La compilación limpia de `rust-ingestor` (`0` cambios sin confirmar en `rust-ingestor`, perfil `--release --locked`) produjo un piloto v2 completo sobre los 4 retailers (`plazavea`, `metro`, `wong`, `tottus`) consumiendo **`56` requests HTTP totales (`56 <= 65`, `0` errores HTTP)**. La auditoría DuckDB sobre `data/bronze` (`359` filas en `4` archivos Parquet ZSTD de `40` columnas) aprueba el gate de Contrato v2 (`scripts/inspect_bronze.py --gate --allow-truncated`, `exit 0`, **`0` violaciones** en `G1..G5`), frente al piloto v1 archivado (`4e96029` en `data/archive/bronze-contract-v1/`), que falla el gate con `G1_CONTRACT_VERSION, G2_EAN_PROVENANCE, G3_AVAILABILITY_DOMAIN` (`exit 1`). En Tottus se eliminaron completamente los `39` EAN sembrados por cruce VTEX y las `39` filas con `available_qty = 1` fabricado (`39 -> 0` EAN observados tras consultar `39` PDPs en vivo; `39 -> 0` filas con `qty = 1`, reemplazadas por `39` observaciones reales `tottus_pdp` y `9` `listed_unknown` sin lectura de caché para disponibilidad). `make verify` pasa al 100% (`98/98` tests Rust + `23/23` tests pytest + `cargo fmt --check` + `cargo clippy -D warnings`).

---

## 1. Suites de Verificación y Gate de Contrato (`make verify` y `--gate`)

| Control | Comando ejecutado | Resultado | Evidencia observada |
| :--- | :--- | :---: | :--- |
| **`make verify` (`verify-rust`)** | `cd rust-ingestor && cargo fmt --check && cargo clippy --all-targets --locked -- -D warnings && cargo test --locked` | **PASS (`exit 0`)** | `0` warnings de `clippy`; **`92/92`** tests unit/integración en `src/lib.rs` + **`6/6`** tests en `src/main.rs` (`98` pasados, `0` fallidos, `0` ignorados). |
| **`make verify` (`verify-py`)** | `.venv/bin/pytest -q` | **PASS (`exit 0`)** | **`23/23`** tests pasados en `2.17s` (`tests/test_inspect_bronze.py`). |
| **Build release limpio** | `git status --porcelain -- rust-ingestor \| wc -l` y `cargo build --release --locked` | **PASS (`exit 0`)** | `0` archivos modificados en `rust-ingestor`; `Finished release profile [optimized]` en `4.33s`; `extractor_version` compilado con `+098958b` (sin `-dirty` ni `+nogit`). |
| **Gate v2 (Bronze fresco)** | `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/bronze --gate --allow-truncated --output-md docs/qa-report-e1.6.md` | **PASS (`gate_exit=0`)** | `has_data: true`, `4` archivos Parquet escaneados, `359` filas bajo contrato `2.0.0`, **`0` violaciones** (`ean_without_source_rows: 0`, `seed_ean_source_rows: 0`, `availability_invalid_rows: 0`, `dirty_or_nogit_version_rows: 0`, `run_statuses: {"truncated": 4}`). |
| **Gate v1 (Archivo `4e96029`)** | `.venv/bin/python scripts/inspect_bronze.py --bronze-dir data/archive/bronze-contract-v1 --gate` | **FAIL (`v1_gate_exit=1`)** | Falló con `G1_CONTRACT_VERSION, G2_EAN_PROVENANCE, G3_AVAILABILITY_DOMAIN` (`contract_versions: {"legacy": 361}`, `ean_without_source_rows: 338`, `availability_invalid_rows: 361`). |

---

## 2. Telemetría Operativa y Métricas del Piloto v2 (`dt=2026-11-14`)

Datos consolidados directamente desde los manifiestos `data/bronze/dt=2026-11-14/retailer=*/metrics-*.json`, las auditorías DuckDB (`scripts/inspect_bronze.py`), los blobs de deduplicación (`data/bronze/blobs/seen_hashes.json`) y la caché PDP (`state/tottus_ean_cache.json`):

### 2.1 Tabla Consolidada por Retailer (Contrato v2 + Manifiestos)

| Retailer | Filas (`row_count`) | `product_observations` | `hash_duplicates_count` | `multi_sku_fanout_count` | `ean_source_counts` | `availability_status_counts` | `gtin_class_counts` | `status` | `stop_reason` por categoría (`categories_detail`) | `peak_ram_mb` | `requests_ok` / `requests_failed` (`requests_error`) |
| :--- | ---: | ---: | ---: | ---: | :--- | :--- | :--- | :--- | :--- | ---: | :---: |
| `plazavea` | 73 | 73 | 0 | 0 | `item_ean`: 57, `reference_id`: 2 | `in_stock`: 69, `out_of_stock`: 4 | `global_candidate`: 48, `missing`: 14, `restricted_circulation`: 6, `bad_length`: 5 | `truncated` | `lacteos`: `exhausted` (`2` págs, `33` prod, `33` sku); `abarrotes`: `max_pages` (`2` págs, `40` prod, `40` sku) | 18.21 | `4` / `0` |
| `metro` | 119 | 118 | 0 | 1 | `item_ean`: 119 | `in_stock`: 118, `out_of_stock`: 1 | `global_candidate`: 106, `restricted_circulation`: 13 | `truncated` | `lacteos`: `max_pages` (`2` págs, `40` prod, `40` sku); `abarrotes`: `max_pages` (`2` págs, `40` prod, `41` sku); `televisor`: `exhausted` (`2` págs, `38` prod, `38` sku) | 30.05 | `6` / `0` |
| `wong` | 119 | 119 | 0 | 0 | `item_ean`: 119 | `in_stock`: 119 | `global_candidate`: 106, `restricted_circulation`: 13 | `truncated` | `lacteos`: `max_pages` (`2` págs, `40` prod, `40` sku); `abarrotes`: `max_pages` (`2` págs, `40` prod, `40` sku); `televisor`: `exhausted` (`2` págs, `39` prod, `39` sku) | 32.37 | `6` / `0` |
| `tottus` | 48 | 48 | 0 | 0 | _None_ | `in_stock`: 39, `listed_unknown`: 9 | `missing`: 48 | `truncated` | `leche`: `max_pages` (`1` pág, `48` prod, `48` sku, `next_offset=20`) | 21.74 | `40` / `0` |
| **TOTAL** | **359** | **358** | **0** | **1** | **`item_ean`: 295, `reference_id`: 2** | **`in_stock`: 345, `listed_unknown`: 9, `out_of_stock`: 5** | **`global_candidate`: 260, `missing`: 62, `restricted_circulation`: 32, `bad_length`: 5** | **4/4 `truncated`** | **`17` páginas totales en `9` categorías** | **máx. 32.37** | **`56` / `0` (`56 <= 65`)** |

### 2.2 Detalle de Manifiestos (`metrics-*.json`) y Artefactos Físicos

| Retailer | `extraction_run_id` | `extractor_version` | `avg_latency_ms` | `mb_transferred` | `total_duration_secs` | Tamaño Parquet (disco) | Esquema / Compresión |
| :--- | :--- | :--- | ---: | ---: | ---: | ---: | :--- |
| `plazavea` | `99bbf366-296b-41cc-8fd7-3855c37f0b8d` | `vtex-catalog-v1.1.0+098958b` | 403.78 | 0.9198 | 2.14 | 160,390 B | 40 cols · 1 row group · `ZSTD` |
| `metro` | `ee45f8b9-dc99-43a1-a899-3f57b5ab4944` | `vtex-catalog-v1.1.0+098958b` | 239.01 | 5.1380 | 2.74 | 967,767 B | 40 cols · 1 row group · `ZSTD` |
| `wong` | `934b994a-a517-46e4-8779-27b5eb03137d` | `vtex-catalog-v1.1.0+098958b` | 380.14 | 5.5035 | 3.12 | 819,535 B | 40 cols · 1 row group · `ZSTD` |
| `tottus` | `3198b41d-ebeb-4a22-98d8-6802e84dca84` | `falabella-listing-v1.1.0+098958b` | 867.74 | 47.1795 | 29.25 | 30,889 B | 40 cols · 1 row group · `ZSTD` |
| **TOTAL** | **4 corridas** | **SHA `098958b` limpio** | **—** | **58.7407** | **—** | **1,978,581 B** | **100% contrato `2.0.0`** |

- **Integridad de metadatos y particionado:**
  - Los 4 manifiestos registran `bronze_contract_version: "2.0.0"`, `business_date: "2026-11-14"` (fecha Lima `UTC-5`), `requested_postal_code: null`, `duplicates_detected: 0` y `error: null`.
  - En los 4 archivos Parquet, `postal_code` y `store_id` tienen `0` valores no nulos (cumpliendo la invariante `applies_geo_context() == false`).
  - `data/bronze/blobs/seen_hashes.json` registra exactamente **`358`** `payload_hash` únicos (`== SUM(distinct_payload_hashes) == product_observations`).
  - `data/bronze/checkpoints/` contiene los `9` checkpoints correspondientes (`plazavea_{abarrotes,lacteos}`, `{metro,wong}_{abarrotes,lacteos,televisor}`, `tottus_leche`).

---

## 3. Comparación Lado a Lado vs. Piloto v1 Archivado (`4e96029` en `data/archive/bronze-contract-v1/`)

El piloto v1 fue archivado intacto mediante `mv` en `data/archive/bronze-contract-v1/` (generado originalmente con `4e96029`, versiones `vtex-catalog-v1.0.0` y `falabella-listing-v1.0.0`).

| Dimensión comparada | Piloto v1 archivado (`4e96029`) | Piloto v2 actual (`098958b`) | Delta / Interpretación verificada |
| :--- | :--- | :--- | :--- |
| **Gate de Contrato v2** | **FAIL (`exit 1`)**: `G1_CONTRACT_VERSION, G2_EAN_PROVENANCE, G3_AVAILABILITY_DOMAIN` | **PASS (`exit 0` con `--allow-truncated`)**: **0 violaciones** (`G1..G5`) | `361` filas `legacy` con `338` EAN sin procedencia y `361` disponibilidades inválidas -> `359` filas `2.0.0` conformes. |
| **Columnas Parquet** | 29 columnas (`legacy`) | 40 columnas (`bronze_contract_version = "2.0.0"`) | Añade `ean_source`, `availability_status`, `availability_source`, contexto seller, unidad de medida, `query_term`, `content_hash`, versión de contrato. |
| **Estado de corrida** | `4/4` reportados como `"success"` pese a truncamiento por `--max-pages` | `4/4` reportados honestamente como `"truncated"` (`categories_detail` por categoría) | Distingue categorías agotadas (`exhausted`: PV `lacteos`, Metro/Wong `televisor`) de cortes por presupuesto (`max_pages`). |
| **Tottus EANs observados** | **`39` / `48` (`81.25%`)** — **39 sembrados** vía cruce VTEX en `state/tottus_ean_cache.json` (`0` de PDPs) | **`0` / `48` (`0.00%`)** — `39` PDPs consultadas en vivo + `9` PDPs ya cacheadas (`48/48` sin GTIN en HTML/JSON-LD) | Confirma empíricamente que las 48 PDPs observadas de `leche` en Tottus omiten GTIN/EAN; las `39` de v1 eran 100% semillas movidas a `state/seeds/tottus_vtex_crossref_seed.json` (`43` candidatos). |
| **Tottus filas con `available_qty = 1`** | **`39` / `48`** (`1` fabricado por mera presencia en listado; `8` con `24`, `1` con `10`) | **`0` / `48`** (`39` vía `tottus_pdp` con `qty` real `2..24`: `32`x`24`, `4`x`10`, `1`x`9`, `1`x`3`, `1`x`2`; `9` con `NULL` y `listed_unknown`) | Se erradicó la fabricación de `available_qty = 1`. Los 9 SKUs cuya PDP no se re-consultó por caché conservan `available_qty = NULL` y `availability_source = "tottus_listing_presence"`. |
| **Requests HTTP totales** | `26` (`PV 4`, `Metro 6`, `Wong 6`, `Tottus 10` = `1 + 9` PDPs) | `56` (`PV 4`, `Metro 6`, `Wong 6`, `Tottus 40` = `1 + 39` PDPs), `0` errores | `+30` requests en Tottus al verificar en vivo las 39 PDPs antes omitidas por las semillas en caché; se mantiene dentro del presupuesto `<= 65`. |
| **Filas totales por retailer** | `metro`: `120`, `plazavea`: `73`, `tottus`: `48`, `wong`: `120` (**Total: `361`**) | `metro`: `119`, `plazavea`: `73`, `tottus`: `48`, `wong`: `119` (**Total: `359`**) | Variación de `-1` fila en Metro (`120 -> 119`) y Wong (`120 -> 119`) por cambio natural del catálogo en vivo (`televisor`: `38` en Metro, `39` en Wong). |
| **Tottus `card_price` (`CMR`)** | `1` oferta (`SKU 129087925` a `20.50`) | `0` ofertas (`raw_payload LIKE '%cmrPrice%'`: `0` filas) | Ausencia verificada directamente en el HTML de listado Tottus al momento de la corrida (`08:34Z`), no atribuible al parser. |
| **Métrica `Hash Dups` vs `1:N Fanout`** | Metro reportó `Hash Dups = 0.83%` bajo cálculo antiguo en `qa-report-e1.md` | Metro: `hash_duplicates_count = 0 (0.00%)`, `multi_sku_fanout_count = 1 (0.84%)` | La métrica corregida separa expansión `1:N` (producto `1021580` con `2` SKUs en `abarrotes`) de duplicados reales de `payload_hash`. |

### Hallazgos Técnicos Clave Observados en v2

1. **Evidencia intradía de `content_hash` frente a `payload_hash` en Metro (`televisor`):**
   - En `metro`, `unique_products = 114` y `distinct_content_hashes = 114`, mientras `product_observations = 118` y `distinct_payload_hashes = 118`.
   - Cuatro productos de `televisor` (`1070392`, `1070399`, `1070408`, `1096860`) aparecieron tanto en la página 1 como en la página 2 del listado VTEX con **2 `payload_hash` distintos** (por rotación de claves volátiles `PriceToken` / `PriceValidUntil` / `CacheVersionUsedToCallCheckout`), pero con **1 único `content_hash` idéntico**. Esto demuestra dentro de una misma corrida que `content_hash` detecta repetición sin cambios de negocio donde `payload_hash` falla.
2. **Cobertura de `availability_source`, contexto de `seller` y unidades VTEX:**
   - **Disponibilidad:** `vtex_offer` cubre `311/311` filas VTEX (`metro`: `118 in_stock`, `1 out_of_stock`; `plazavea`: `69 in_stock`, `4 out_of_stock`; `wong`: `119 in_stock`). En Tottus, `39` filas registran `tottus_pdp` (`in_stock`) y `9` registran `tottus_listing_presence` (`listed_unknown`, `qty = NULL`).
   - **Sellers:** Todos los SKUs VTEX (`311/311`) pueblan `seller_name`, `seller_count` e `is_default_seller`. Se observaron ofertas multi-seller (`seller_count > 1`) en **`37` filas de Metro** (`CENCOSUD RETAIL PERU S.A.`, `wongiononfood`) y **`7` filas de Plaza Vea** (`Plaza Vea`, `VTC PERU`, `oechsle`, `FRUTOLAR`, `MAGNESOL`, `BIOCENTER NATURAL`).
   - **Unidades de venta (`measurement_unit`, `unit_multiplier`):** Bronze conserva precios crudos sin multiplicar: Metro reporta `1` producto pesable (`sku=3668`, `"Queso Fresco Light Cuisine & Co x kg"`, `measurement_unit="kg"`, `unit_multiplier=0.1`, `selling_price=30.50`) y Wong reporta `4` (`sku=3484, 3559, 3668` con `0.1`, `sku=3678` con `0.4`).
3. **Actualización verificada de `state/tottus_ean_cache.json`:**
   - Antes del piloto v2 contenía `9` entradas `tottus_pdp_ssr` (`pdp_checked: true`, `ean: null`). Tras consultar las `39` PDPs restantes en el piloto v2, pasó a **`48` entradas**, todas con `source: "tottus_pdp_ssr"`, `pdp_checked: true` y `ean: null` (sin ninguna semilla VTEX).

---

## 4. Auditoría Layer Bronze v2 (`scripts/inspect_bronze.py --bronze-dir data/bronze`)

### Coverage, Deduplication & Stock Summary

| Scope | Rows | Products | SKUs | SKU/Prod | EAN no nulo (%) | EAN Mod-10 (%) | Dup Flag (%) | Hash Dups (%) | 1:N Fanout (%) | Null `raw_payload` (%) | In Stock (%) | Card < Selling (%) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | 119 | 114 | 115 | 1.01 | 100.00% | 100.00% | 0.00% | 0.00% | 0.84% | 0.00% | 99.16% | 31.09% |
| plazavea | 73 | 73 | 73 | 1.00 | 80.82% | 73.97% | 0.00% | 0.00% | 0.00% | 0.00% | 94.52% | 1.37% |
| tottus | 48 | 48 | 48 | 1.00 | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% | 0.00% | 81.25% | 0.00% |
| wong | 119 | 119 | 119 | 1.00 | 100.00% | 100.00% | 0.00% | 0.00% | 0.00% | 0.00% | 100.00% | 31.09% |
| **TOTAL** | 359 | 354 | 355 | 1.00 | 82.73% | 81.34% | 0.00% | 0.00% | 0.28% | 0.00% | 96.10% | 20.89% |

### Contract v2, Provenance & GTIN Summary

- **Contract Versions:** `2.0.0`: 359
- **Run Statuses:** `truncated`: 4
- **Gate Violations Counters:** `ean_without_source_rows`: 0, `seed_ean_source_rows`: 0, `availability_invalid_rows`: 0, `dirty_or_nogit_version_rows`: 0

| Scope | Product Obs | GTIN Classes | EAN Sources | Availability Status |
| :--- | ---: | :--- | :--- | :--- |
| metro | 118 | `global_candidate`: 106, `restricted_circulation`: 13 | `item_ean`: 119 | `in_stock`: 118, `out_of_stock`: 1 |
| plazavea | 73 | `global_candidate`: 48, `missing`: 14, `restricted_circulation`: 6, `bad_length`: 5 | `item_ean`: 57, `reference_id`: 2 | `in_stock`: 69, `out_of_stock`: 4 |
| tottus | 48 | `missing`: 48 | _None_ | `in_stock`: 39, `listed_unknown`: 9 |
| wong | 119 | `global_candidate`: 106, `restricted_circulation`: 13 | `item_ean`: 119 | `in_stock`: 119 |
| **TOTAL** | 358 | `global_candidate`: 260, `missing`: 62, `restricted_circulation`: 32, `bad_length`: 5 | `item_ean`: 295, `reference_id`: 2 | `in_stock`: 345, `listed_unknown`: 9, `out_of_stock`: 5 |

### Card Programs Breakdown

| Scope | Program | Active Offers |
| :--- | :--- | ---: |
| metro | Cencosud | 37 |
| plazavea | Oh! | 1 |
| tottus | _None_ | 0 |
| wong | Cencosud | 37 |
| **TOTAL** | Cencosud | 74 |
| **TOTAL** | Oh! | 1 |

### Price Distribution (PEN)

| Scope | Metric | Non-Null | Min | P25 | P50 | Mean | P75 | Max |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| metro | `list_price` | 118 | 1.90 | 6.85 | 22.90 | 895.61 | 1499.00 | 6999.00 |
| metro | `selling_price` | 118 | 1.00 | 6.50 | 19.90 | 680.76 | 1136.50 | 5899.00 |
| metro | `card_price` | 37 | 399.00 | 1149.00 | 1649.00 | 2020.35 | 2949.00 | 5699.00 |
| plazavea | `list_price` | 72 | 1.60 | 4.47 | 6.50 | 13.37 | 18.77 | 78.50 |
| plazavea | `selling_price` | 72 | 1.60 | 4.30 | 5.80 | 12.92 | 18.05 | 78.50 |
| plazavea | `card_price` | 1 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 | 4.90 |
| tottus | `list_price` | 36 | 4.20 | 6.40 | 18.55 | 17.41 | 24.73 | 49.20 |
| tottus | `selling_price` | 48 | 2.50 | 5.15 | 13.40 | 13.80 | 21.50 | 44.90 |
| tottus | `card_price` | 0 | N/A | N/A | N/A | N/A | N/A | N/A |
| wong | `list_price` | 119 | 1.90 | 6.55 | 21.50 | 872.24 | 1549.00 | 6999.00 |
| wong | `selling_price` | 119 | 1.80 | 5.85 | 19.90 | 654.22 | 1174.00 | 5899.00 |
| wong | `card_price` | 37 | 399.00 | 1149.00 | 1449.00 | 1953.59 | 2499.00 | 5699.00 |
| **TOTAL** | `list_price` | 345 | 1.60 | 5.90 | 18.20 | 611.79 | 36.00 | 6999.00 |
| **TOTAL** | `selling_price` | 357 | 1.00 | 5.20 | 16.20 | 447.55 | 25.90 | 5899.00 |
| **TOTAL** | `card_price` | 75 | 4.90 | 1124.00 | 1449.00 | 1960.55 | 2724.00 | 5699.00 |

### Sample Multi-Retailer Observations

| Retailer | SKU | Producto | P. Lista | P. Venta | P. Tarjeta + Programa | EAN | Stock |
| :--- | :--- | :--- | ---: | ---: | :--- | :--- | ---: |
| metro | 39308719 | Televisor Smart TV TCL 55" QDMINILED 120HZ 55C6K | 2149.00 | 1849.00 | 1749.00 (Cencosud) | 6921732845582 | 99999 |
| plazavea | 10672464 | Filete de Atún CAMPOMAR en Aceite Vegetal Lata 150g | 5.90 | 5.20 | 4.90 (Oh!) | 7750408003235 | 2090 |
| wong | 39312006 | Televisor Smart TV TCL 55" QDMINILED 120HZ 55C6K Smart TV TCL 55" QDMINILED 120HZ 55C6K | 2149.00 | 1849.00 | 1749.00 (Cencosud) | 6921732845582 | 99999 |
| tottus | 113706451 | Mezcla Láctea Cremosita Sixpack Lata 390 g | 24.60 | 21.90 | N/A | N/A | 24 |
| metro | 39308726 | Televisor Smart TV TCL 65" QDMINILED 120HZ 65C6K | 2799.00 | 2299.00 | 2199.00 (Cencosud) | 6921732845599 | 100 |
| wong | 39312013 | Televisor Smart TV TCL 65" QDMINILED 120HZ 65C6K Smart TV TCL 65" QDMINILED 120HZ 65C6K | 2799.00 | 2299.00 | 2199.00 (Cencosud) | 6921732845599 | 100 |
| plazavea | 10033838 | Azúcar Rubia BELL'S Bolsa 5Kg | 17.90 | 17.90 | N/A | 2200201985535 | 176 |
| tottus | 113708427 | Leche Entera en Polvo Bolsa 96 g | 4.50 | 4.49 | N/A | N/A | 24 |
| metro | 39308729 | Televisor Smart TV TCL 75" QDMINILED 120HZ 75C6K | 3999.00 | 3249.00 | 3099.00 (Cencosud) | 6921732845605 | 10 |
| wong | 39312016 | Televisor Smart TV TCL 75" QDMINILED 120HZ 75C6K Smart TV TCL 75" QDMINILED 120HZ 75C6K | 3999.00 | 3249.00 | 3099.00 (Cencosud) | 6921732845605 | 10 |
| plazavea | 10207333 | Mezcla Láctea en Polvo ANCHOR Lata 800g | 33.00 | 33.00 | N/A | 7613287384003 | 30 |
| tottus | 113709314 | Leche en Polvo Lata 800 g | 36.50 | 36.20 | N/A | N/A | 10 |
| metro | 39308735 | Televisor Smart TV TCL 85" QDMINILED 120HZ 85C6K | 6199.00 | 4499.00 | 4299.00 (Cencosud) | 6921732845612 | 10 |
| wong | 39312022 | Televisor Smart TV TCL 85" QDMINILED 120HZ 85C6K Smart TV TCL 85" QDMINILED 120HZ 85C6K | 6199.00 | 4499.00 | 4299.00 (Cencosud) | 6921732845612 | 10 |
| plazavea | 10207334 | Bebida de Soya UHT SOY VIDA Caja 946ml | 6.50 | 6.50 | N/A | 7751271030786 | 208 |
| tottus | 129087754 | Leche Zero Lacto Sin Lactosa Lata 390 g | N/A | 4.60 | N/A | N/A | 24 |

---

## 5. No verificado

Los siguientes aspectos **no fueron ejecutados, medidos ni verificados** dentro del alcance de los Pasos 1–6 de E1.6 y quedan explícitamente fuera de los logros declarados:

- **Repetibilidad de hashes en Día 2 (Paso 9):** No se ejecutó una segunda corrida en un día calendario posterior de Lima (`>= 1` día después) para medir la tasa de repetición inter-día entre `payload_hash` y `content_hash` (pendiente de visto bueno del propietario).
- **Revisión legal de Términos de Servicio (ToS) y `robots.txt`:** No se realizó análisis jurídico de ToS ni inspección formal de `robots.txt` para `www.plazavea.com.pe`, `www.metro.pe`, `www.wong.pe` y `tottus.falabella.com.pe`.
- **Paginación multi-página de Tottus más allá de la página 1:** El piloto ejecutó únicamente `--max-pages 1` en `leche`; no se validó en vivo la paginación `page=2..N` en Tottus ni otras categorías de Tottus con `--enrich-tottus-pdp`.
- **Precios autenticados de Wong Prime:** El ingestor solo consulta endpoints públicos sin login; no se validaron ofertas que requieran sesión o membresía autenticada (como tarifas exclusivas de Wong Prime no expuestas en teasers públicos).
- **Backfill de catálogo completo fuera del presupuesto del piloto:** Todas las corridas del piloto estuvieron acotadas a `<= 65` requests (`--page-size 20 --max-pages 2` en VTEX, `--max-pages 1` en Tottus); no se midieron tiempos, memoria pico ni tasa de errores/bloqueos en corridas de catálogo completo ni la reanudación real (`--resume`) sobre múltiples páginas en producción.
- **Regionalización efectiva (`--postal-code` / `store_id`):** Ningún adaptador aplica contexto geográfico a las consultas remotas (`applies_geo_context() == false`); la variación de precios o stock por tienda/ubigeo no ha sido verificada.
- **Carga a PostgreSQL (DDL) y capas Silver/Gold (E2/E3):** No se ejecutó ingesta a PostgreSQL ni evaluación de matching de `state/seeds/tottus_vtex_crossref_seed.json`.
