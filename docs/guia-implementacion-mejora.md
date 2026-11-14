# Guía de implementación y mejora — retail-intel-pe

**Fecha:** 2026-11-14 · **Base:** `main` @ `b0316bc` (tags `v0.1.0`, `v0.1.5`; código idéntico al auditado en `4e96029`) · **Origen:** auditoría técnica independiente del 14-nov-2026 (veredicto: *APPROVED WITH CONDITIONS*)

Esta guía es el documento de trabajo del proyecto: dice qué se arregla, en qué orden, con qué librerías ya instaladas, quién lo hace (agentes y skills) y qué evidencia cierra cada fase. Cada fase se ejecuta con un plan Superpowers propio; el de la fase vigente es `docs/superpowers/plans/2026-11-14-e1.6-bronze-contract-integrity.md`.

Todo dato citado aquí salió del código en `4e96029` (`b0316bc` solo añade documentación) o del Bronze real en `data/bronze/dt=2026-11-14/` (361 filas, 4 retailers). Lo que no se midió está marcado como **no verificado**.

---

## 1. Estado de partida

| Aspecto | Estado verificado |
| --- | --- |
| Código | Ingestor Rust (`rust-ingestor/crates/ingestor`, v0.1.0) con adaptadores VTEX (Plaza Vea, Metro, Wong) y Falabella/Tottus; `scripts/inspect_bronze.py`; DDL PostgreSQL borrador |
| Pruebas | 103 + 6 Rust, 23 pytest tras reconciliar con `main` |
| Datos | Piloto de 26 requests: Metro 120, Wong 120, Plaza Vea 73, Tottus 48 filas |
| Repositorio | Remoto `origin` en GitHub, **público** desde el 14-nov-2026 (`main` y tags publicados). `Cargo.lock` versionado (ya no ignorado). Entorno Python sin declarar (venv creado con `uv`, sin `pyproject` ni lock) |
| Trabajo paralelo | `rapid-stone` fusionado en `main` (`541f567`) y reconciliado en E1.6 (`docs/superpowers/plans/2026-11-14-e1.6-reconcile-main.md`) |
| Fases Silver/Gold/ER | No existen todavía (correcto: no se debe construir encima de un Bronze con defectos de contrato) |

Puntajes de la auditoría (0–10): Arquitectura 6.5 · Ingesta 6 · Data Engineering 5 · Calidad de datos 3.5 · Entity Resolution 2.5 · Retail Analytics 2 · Testing 6 · Escalabilidad 3 · Mantenibilidad 6 · Producción 2.

## 2. Reglas del proyecto

1. **Bronze guarda lo observado, con su procedencia.** Nada inferido, sembrado ni heredado de una caché sin fuente. Toda derivación (clasificación de GTIN, precio por unidad, promociones, inferencia de stock) vive en Silver con `rule_version`.
2. **Evidencia antes que afirmación.** Ningún reporte declara una métrica que no salga de un comando ejecutado en esa sesión; se pega el comando y su salida.
3. **Contrato versionado.** Cambiar columnas o semántica de Bronze sube `bronze_contract_version`; cambiar extracción sube la versión del crate. Ningún archivo Bronze se reescribe: se archiva.
4. **Recolección acotada y declarada.** Solo endpoints públicos de catálogo, ≤ 2 req/s, presupuesto de requests escrito antes de correr. Sin login, sin evasión de controles.
5. **Sin dependencias nuevas sin justificación escrita** (problema, alternativa con lo instalado, costo). La mayoría de lo pendiente se resuelve con lo que ya está.
6. **Decisiones del propietario** (sección 9): no se toman por él. En particular: push de ramas (el repo es público: todo lo que se sube queda publicado), identificación del User-Agent, regionalización, dependencias nuevas.
7. **TDD y revisión independiente.** Cada tarea: test que falla → implementación → `make verify` → commit. Cada fase cierra con revisión de rama por un agente que no escribió el código y con una re-auditoría de solo lectura.

## 3. Hallazgos que gobiernan el trabajo

| ID | Hallazgo | Evidencia | Se resuelve en |
| --- | --- | --- | --- |
| P0-1 | Los 39 EAN de Tottus en Bronze son semillas (`vtex_crossref_gtin13_seed`, `pdp_checked: false`), no observaciones; además `record_pdp_enrichment` conserva la semilla y la re-etiqueta como `tottus_pdp_ssr` | `state/tottus_ean_cache.json` (43 semillas); `falabella.rs:94-123`, `:198-203` | E1.6 T4 |
| P0-2 | Disponibilidad de Tottus fabricada (`Some(1)` por presencia; `maxOnCartAmount` como stock) y agotado "pegajoso" desde caché | `falabella.rs:404-427`, `:204-209`; 39/48 filas con qty=1 | E1.6 T5 |
| P0-3 | `Cargo.lock` sin versionar (el remoto ya existe desde el 14-nov) | `.gitignore` | E1.6 T1 |
| P1-1 | Dedup por `payload_hash` inefectivo en VTEX: el payload trae `PriceToken` (JWT de 30 min), `PriceValidUntil`, `CacheVersionUsedToCallCheckout` | Wong SKU `39345952` dos veces en una corrida con payload distinto solo en esos campos; `duplicates_detected = 0` | E1.6 T2 (columna `content_hash`) + medición día 2 |
| P1-2 | `postal_code`/`store_id` se escriben sin haberse enviado al retailer | Nunca aparecen en la URL (`vtex.rs:409-424`, `falabella.rs:803-818`) | E1.6 T7; regionalización real = investigación E2+ |
| P1-3 | Lógica inferida en Bronze sin procedencia (EAN por fallback, tarjeta por substring, stock) | `vtex.rs:118-166`, `:251-278` | E1.6 T4–T6 (procedencia); derivación → E2 |
| P1-4 | GTIN sin clasificar; `gtin UNIQUE` en el DDL | 13/120 Metro y 13/120 Wong con prefijo `2`; Plaza Vea: 6 de circulación restringida, 5 de longitud inválida; mismo EAN en 2 SKUs Metro | E2 (clasificación) + E3 (rediseño DDL) |
| P1-5 | Precio sin base de unidad ni seller | 5 SKUs `measurementUnit: kg` con `unitMultiplier` 0.1/0.4; 38 ítems Metro y 7 Plaza Vea con 2 sellers | E1.6 T6 |
| P1-6 | Completitud no medible: cortar por `--max-pages` reporta `success` | `runner.rs:91`, `:171-175`; 7 de 9 checkpoints `completed: false` con estado `success` | E1.6 T8 |
| P1-7 | Toda la corrida en memoria y un solo commit | `runner.rs:48`, `:111`, `:157` | E1.6 T8 (streaming); checkpoint por chunk → escalamiento |
| P1-8 | `extractor_version` fijo entre E1 y E1.5 | `vtex.rs:406`, `falabella.rs:800` | E1.6 T3 |
| P1-9 | DDL PostgreSQL nunca ejecutado en pruebas; `dim_comparison_group` desconectada | `sql/postgres_schema.sql` | E3 |
| P2 | `dt` en UTC (20:35 Lima cae en el día siguiente); `inspect_bronze` con métricas confundidas y sin código de salida; `mb_transferred` mide bytes decodificados; productos sin SKU descartados y página abortada por un `productId` faltante; HTTP sin jitter ni `Retry-After`; sin CI | detalle en el informe de auditoría | E1.6 T1, T6–T9; resto en backlog (sección 8) |

## 4. Contrato Bronze v2 (objetivo de E1.6)

Se conservan las 29 columnas actuales en su posición y se **agregan 11 al final**, en este orden:

| # | Columna | Tipo Arrow | Null | Valores / regla |
| --- | --- | --- | --- | --- |
| 30 | `ean_source` | Utf8 | sí | `item_ean` · `reference_id` · `product_reference` · `spec` (VTEX); `listing` · `pdp` (Tottus). `NULL` ⇔ `ean` es `NULL` |
| 31 | `availability_status` | Utf8 | no | `in_stock` · `out_of_stock` · `listed_unknown` · `unknown` |
| 32 | `availability_source` | Utf8 | no | `vtex_offer` · `tottus_listing_flag` · `tottus_listing_qty` · `tottus_listing_presence` · `tottus_pdp` · `none` |
| 33 | `seller_name` | Utf8 | sí | nombre del seller elegido |
| 34 | `seller_count` | Int32 | sí | sellers que ofrecen el SKU en el payload |
| 35 | `is_default_seller` | Boolean | sí | el seller elegido venía marcado `sellerDefault` |
| 36 | `measurement_unit` | Utf8 | sí | `measurementUnit` de VTEX, tal cual |
| 37 | `unit_multiplier` | Float64 | sí | `unitMultiplier` de VTEX, tal cual (Bronze no multiplica precios) |
| 38 | `query_term` | Utf8 | no | término de categoría/búsqueda que produjo la fila |
| 39 | `content_hash` | Utf8 | no | SHA-256 del payload sin claves volátiles |
| 40 | `bronze_contract_version` | Utf8 | no | `2.0.0` |

Cambios de semántica sobre columnas existentes:

- `ean`: solo valores observados en el payload de esa corrida o en una PDP realmente consultada. Las semillas salen de la caché y pasan a `state/seeds/` como *candidatos para evaluar matching en E3*.
- `available_qty`: solo cantidades que el retailer expone. `99999` de VTEX se conserva tal cual (Silver lo trata como centinela). Nunca se fabrica `1`.
- `postal_code` / `store_id`: `NULL` mientras el adaptador no los aplique de verdad a la consulta. El valor pedido queda en el manifiesto de la corrida.
- `extractor_version`: `<familia>-v<versión del crate>+<sha7>[-dirty]`.
- Partición `dt`: fecha de negocio en Lima (UTC−5 fijo), no UTC.
- Estado de corrida: `success` · `truncated` · `partial_error`, con detalle por categoría (`stop_reason`: `exhausted` · `max_pages` · `error`).

## 5. Recetario de librerías instaladas

Todo lo de esta sección se ejecutó el 14-nov-2026 contra las versiones instaladas. No requiere instalar nada.

### 5.1 Rust — `parquet`/`arrow` 53.4, `serde_json` 1.0, `sha2`

**Escritura incremental** (resuelve P1-7). `ArrowWriter` acepta varios `RecordBatch` y corta row groups solo; no hace falta juntar la corrida en un `Vec`:

```rust
let props = WriterProperties::builder()
    .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
    .set_max_row_group_size(2_000)
    .build();
let mut w = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
w.write(&batch_pagina_1)?;          // una llamada por página
w.write(&batch_pagina_2)?;
let file = w.into_inner()?;         // cierra el footer
file.sync_all()?;                   // luego rename atómico
```

Probado: dos `write` + `flush` producen 2 row groups legibles con `ParquetRecordBatchReaderBuilder`; `w.in_progress_size()` y `w.flushed_row_groups()` sirven para telemetría.

**Hash de contenido** (resuelve P1-1). `serde_json` está compilado sin `preserve_order`, así que los objetos se serializan con claves ordenadas: quitar las claves volátiles y re-serializar da un hash estable e independiente del orden.

```rust
fn strip_volatile(v: &mut Value, keys: &[&str]) {
    match v {
        Value::Object(m) => { for k in keys { m.remove(*k); } m.values_mut().for_each(|c| strip_volatile(c, keys)); }
        Value::Array(a) => a.iter_mut().for_each(|c| strip_volatile(c, keys)),
        _ => {}
    }
}
```

Probado: dos payloads que difieren solo en `PriceToken`, `PriceValidUntil`, `CacheVersionUsedToCallCheckout` y en el orden de claves dan el mismo hash; un cambio de `Price` da otro.

**Versión de extractor estática** (resuelve P1-8). El trait devuelve `&'static str`; `concat!` con `env!` lo mantiene estático: `concat!("vtex-catalog-v", env!("CARGO_PKG_VERSION"), "+", env!("INGESTOR_GIT_SHA"))`, con `INGESTOR_GIT_SHA` emitido por `build.rs` (`cargo:rustc-env=`).

**Fecha de negocio.** Perú no tiene horario de verano: `chrono::FixedOffset::west_opt(5 * 3600)` basta; `chrono-tz` no hace falta.

### 5.2 DuckDB 2.0.0 (Python)

- **Sintaxis de lambda:** la flecha simple `x -> ...` ya falla (`Binder Error: Deprecated lambda arrow`). Usar `lambda x: ...`.
- **Zona horaria:** la extensión `icu` viene cargada; `timezone('America/Lima', scraped_at)` funciona. Lo que falla es *traer* un `TIMESTAMPTZ` a Python sin `pytz`: convertir a `VARCHAR`/`DATE` dentro del SQL.
- **Extensión `postgres`:** no está instalada (`INSTALL` requiere red). Usarla es una dependencia nueva: decidirlo en E3.

```sql
-- Clasificación de GTIN (va en Silver, no en Bronze)
CREATE MACRO gtin_class(e) AS CASE
  WHEN e IS NULL THEN 'missing'
  WHEN NOT regexp_full_match(e, '[0-9]+') THEN 'non_numeric'
  WHEN length(e) NOT IN (8, 12, 13, 14) THEN 'bad_length'
  WHEN length(e) = 13 AND e[1] = '2' THEN 'restricted_circulation'
  WHEN length(e) = 8 AND e[1] IN ('0', '2') THEN 'restricted_circulation'
  ELSE 'global_candidate' END;

-- Dígito verificador GS1 Mod-10
CREATE MACRO gtin_mod10_ok(e) AS (
  (10 - (list_sum(list_transform(range(1, length(e)),
        lambda i: CAST(e[length(e) - i] AS INT) * (CASE WHEN i % 2 = 1 THEN 3 ELSE 1 END))) % 10)) % 10
) = CAST(e[length(e)] AS INT);

-- GTIN-14 canónico:  lpad(ean, 14, '0')
-- Campos del payload VTEX por SKU
WITH p AS (SELECT retailer, sku_id, raw_payload::JSON j FROM bronze WHERE raw_payload IS NOT NULL),
     it AS (SELECT retailer, sku_id, unnest(from_json(j->'items', '["JSON"]')) item FROM p)
SELECT retailer, item->>'measurementUnit', CAST(item->>'unitMultiplier' AS DOUBLE),
       json_array_length(item->'sellers'),
       item->'sellers'->0->'commertialOffer'->>'IsAvailable'
FROM it WHERE item->>'itemId' = sku_id;
```

Resultado real de `gtin_class` sobre el piloto: Metro 107 `global_candidate` / 13 `restricted_circulation`; Wong igual; Plaza Vea 48 / 6 restringidos / 5 `bad_length` / 14 `missing`; Tottus 39 (semillas) / 9 `missing`.

Similitud de texto ya incluida (suficiente para generar candidatos en E3 sin embeddings): `strip_accents`, `jaro_winkler_similarity`, `jaccard`, `levenshtein`.

### 5.3 Polars 2.1.0

Hoy tiene 0 imports en el repo. Su lugar es E2: parsers fila a fila de cantidad, unidad y multipack. Esqueleto probado sobre el Bronze real:

```python
QTY = r"(?i)(\d+(?:[.,]\d+)?)\s*(kg|g|gr|ml|l|lt|un|und|unid|unidades)\b"
lf = pl.scan_parquet("data/bronze/dt=*/retailer=*/part-*.parquet", hive_partitioning=True)
df = (lf.with_columns(pl.col("raw_name").str.extract_groups(QTY).alias("q"))
        .with_columns(
            pl.col("q").struct.field("1").str.replace(",", ".").cast(pl.Float64, strict=False).alias("net_qty"),
            pl.col("q").struct.field("2").str.to_lowercase()
              .replace({"gr": "g", "lt": "l", "un": "und", "unid": "und", "unidades": "und"}).alias("net_qty_unit"))
        .collect())
df.write_parquet(path, compression="zstd", compression_level=3)
```

Línea base de esta regla ingenua (un solo regex): cantidad extraída en Tottus 48/48, Plaza Vea 69/73, Metro 83/120, Wong 80/120. Los fallos de Metro/Wong incluyen los televisores del término `televisor` y nombres sin gramaje (`Pack x6 Leche Evaporada Gloria Entera`, `Queso Fresco ... x kg`): son los casos que el catálogo de reglas de E2 debe cubrir o marcar `pending_rule`.

Reparto recomendado: **Polars** para parsers y reglas por fila; **DuckDB** para JSON, joins, agregados, lectura/escritura particionada y auditorías.

## 6. Equipo: agentes y skills

Disponibles en Antigravity (`agy agents`, `agy plugin list`, verificado 14-nov-2026):

| Rol | Agente | Skills que usa | Tareas |
| --- | --- | --- | --- |
| Orquestación | sesión principal de `agy` | `superpowers:subagent-driven-development`, `using-git-worktrees`, `finishing-a-development-branch` | reparte tareas, mantiene el plan, no escribe código de tarea |
| Rust (ingestor) | `rust-engineer` (definición de proyecto) | `test-driven-development`, `systematic-debugging` | E1.6 T2–T8 |
| Python / DuckDB / Polars | `va-python-pro`, `va-data-engineer`, `va-sql-pro` | `test-driven-development` | E1.6 T1, T9; E2 completo |
| QA y verificación | `va-qa-expert`, `va-test-automator` | `verification-before-completion` | pilotos, gates, reportes QA con salida real |
| Revisión independiente | `va-code-reviewer`, `va-surgeon-reviewer` | `requesting-code-review`, `receiving-code-review` | revisión por tarea y de rama; nunca el autor del código |
| Diagnóstico y rendimiento | `va-debugger`, `va-performance-engineer` | `systematic-debugging` | fallos de pruebas; escalera de benchmark |
| Análisis de datos | `va-data-analyst`, `va-data-scientist` | `exploratory-data-analysis`, `statistical-analysis` (skills de proyecto) | perfilado de Silver; benchmark etiquetado de E3 |
| Dependencias, git, docs | `va-dependency-manager`, `va-git-workflow-manager`, `va-documentation-engineer` | — | lockfiles, ramas, contrato y política |

**Aprovisionamiento pendiente.** Antigravity no trae un agente Rust (`va-*` no lo incluye). Existe una definición en `/home/balonzifer/.herdr/worktrees/tracker_retail_prices/worktree-rapid-stone-5ff6/.agents/` (10 agentes, entre ellos `rust-engineer`, y los skills `exploratory-data-analysis` y `statistical-analysis`), sin versionar. Copiar ese directorio a `.agents/` en la raíz del repo y versionarlo. Si tras copiarlo `agy agents` no lista `rust-engineer`, las tareas Rust las toma la sesión principal con el skill TDD (**no verificado** que `agy` descubra `.agents/agents/`).

**Auditor externo.** Claude Code hace la re-auditoría de solo lectura al cierre de cada fase (mismo método que la del 14-nov: reproducir pruebas en copia aislada y contrastar afirmaciones con el Bronze real). No participa en la implementación, para conservar independencia.

## 7. Fases y criterios de salida

### 7.0 Trabajo paralelo reconciliado

**Reconciliado (14-nov-2026):** `worktree/rapid-stone-5ff6` se fusionó en `main` (`541f567`, seguido de `.gitignore` de `data/` en `c9b269b`) y quedó integrado en `feat/e1.6-bronze-contract` mediante merge (`--no-ff`) siguiendo `docs/superpowers/plans/2026-11-14-e1.6-reconcile-main.md`. **Conservado de `main`:** deduplicación con unión en disco al persistir (`PayloadDeduplicator::persist`), respeto de `Retry-After` (tope 30 s) en `http.rs` (sin jitter), expiración diaria de checkpoints, guardas de productos sin SKU (sin marcar hash en `runner.rs`) y sin `productId` en VTEX/Tottus, parser de precios peruanos en Tottus (`S/`, comas y rangos), `Cargo.lock` versionado y compatibilidad MSRV 1.82. **Corregido en la reconciliación (hallazgos 1–5):** ubicación de `CategoryOutcome` solo dentro de `if cp.completed` en `runner.rs` (sin saltar checkpoints vencidos en `else`), validación y log de expiración de checkpoints por fecha de negocio `America/Lima` (UTC-5), reconocimiento de coma como separador de miles en precios peruanos sin decimales (`1,299`, `12,999`, `1,299,000`), eliminación de parámetros geográficos sin verificar (`_postalCode`/`_storeId` en VTEX y `postalCode`/`storeId` en Tottus) en `fetch_page`, y conteo de ítems rechazados de Tottus en `products_rejected` y `has_more_pages`.

### E1.6 — Integridad del contrato Bronze (vigente)

Plan: `docs/superpowers/plans/2026-11-14-e1.6-bronze-contract-integrity.md` (10 tareas).

Sale cuando: (1) `make verify` verde; (2) piloto regenerado con contrato v2 pasa `inspect_bronze.py --gate`; (3) 0 EAN sembrados y 0 stock fabricado en Bronze; (4) revisión de rama sin hallazgos bloqueantes; (5) corrida de día 2 medida (tasa de repetición por `payload_hash` vs `content_hash`); (6) re-auditoría independiente.

### E2 — Silver (normalización estructural)

Decisiones previas (escribirlas antes del plan, con `superpowers:brainstorming` → `writing-plans`):

- **Grano y clave de la observación**: propuesta `(retailer, sku_id, seller_id, business_date, extraction_run_id)`; definir qué pasa con dos observaciones del mismo SKU en una corrida.
- **Llave de dedup**: con la medición de día 2, decidir si `is_duplicate_payload` pasa a basarse en `content_hash` y si el payload va a un almacén aparte direccionado por hash.
- **Catálogo de reglas versionado** (`config/normalization_rules_v1.yaml`): marcas, unidades, multipacks, formas; prohibido convertir g↔ml sin densidad.

Alcance: clasificación y normalización de GTIN (macros de 5.2, GTIN-14); precio por unidad usando `measurement_unit`/`unit_multiplier` y el contenido parseado; derivación de tarjeta/promoción (hoy en Bronze por substring) con `rule_version`; estado de disponibilidad derivado (centinela `99999`, `listed_unknown`); `scripts/inspect_silver.py` como gate.

Sale cuando: muestra de **≥ 5 000 SKUs reales por retailer**; cobertura de `normalization_status = ok` medida por categoría; muestra manual de 200 filas con error de parseo < 2 %; pruebas basadas en propiedades para el parser (requiere decidir `hypothesis`: dependencia nueva).

### E3 — Entity resolution

Orden obligatorio: **primero el benchmark, después el matcher.**

1. Conjunto etiquetado de ~300 pares con negativos difíciles (misma marca, distinto gramaje; multipack vs unidad; el caso Metro "Gelatina 120 g / 130 g" con el mismo EAN).
2. Rediseño del DDL (`gtin` deja de ser `UNIQUE`; `dim_comparison_group` conectada; DDL ejecutado en una base efímera dentro de las pruebas).
3. Cascada: determinista (GTIN global válido) → estructural (marca + contenido + forma) → candidatos por similitud (`jaro_winkler_similarity`, `jaccard`) → abstención y revisión humana (`match_review_log`).
4. Embeddings solo si el benchmark muestra que la cascada no alcanza; evaluar Splink sobre DuckDB antes que modelos.

Línea base hoy: 250 EAN distintos, 77 compartidos entre retailers, pero 55 son solo Metro↔Wong (mismo grupo) y 12 involucran semillas de Tottus; **solo 10 son coincidencias independientes Plaza Vea↔Cencosud**. Las semillas archivadas en `state/seeds/` sirven aquí como candidatos a verificar, nunca como verdad.

Sale cuando: precisión ≥ 0.98 en matches automáticos sobre el benchmark, con recall y tasa de abstención reportados.

### Escalera de benchmark (transversal)

250 → 5 000 → 25 000 → 100 000 SKUs, contra un **servidor local de replay** de payloads reales (no contra los retailers). En cada peldaño: `peak_ram_mb`, MB por 1 000 filas, tiempo de escritura, tamaño comprimido. Dato de partida: ≈ 45–48 KB de payload por fila en Metro/Wong (≈ 3.3 KB comprimido), de los cuales `PaymentOptions` + `Installments` son ≈ 24 KB. El checkpoint por chunk (varios archivos por corrida) se implementa al llegar a 25 000.

## 8. Backlog priorizado

| # | Ítem | Prio | Esfuerzo | Estado / dónde |
| --- | --- | --- | --- | --- |
| 1 | Commit, tags, `Cargo.lock`, remoto | P0 | XS | commit, tags y remoto hechos (14-nov); lock → E1.6 T1 |
| 2 | Quitar siembra de EAN de Bronze + `ean_source` | P0 | S | E1.6 T4 |
| 3 | `availability_status` + fuente; quitar override de caché | P0 | S–M | E1.6 T5 |
| 4 | Versión de extractor = versión del crate + SHA | P1 | XS | E1.6 T3 |
| 5 | Corrida de día 2 para medir dedup; diseñar `content_hash` + almacén de payloads | P1 | M | columna en E1.6 T2; medición T10; diseño en E2 |
| 6 | Contrato v2: unidad de venta, seller, término de consulta, versión | P1 | M | E1.6 T2, T6 |
| 7 | Manifiesto de corrida + estado `truncated` | P1 | M | E1.6 T8 |
| 8 | Escritura por chunks + checkpoint por chunk | P1 | M | streaming en E1.6 T8; checkpoint por chunk en escalamiento |
| 9 | No escribir contexto geográfico no aplicado; investigar regionalización | P1 | XS + L | E1.6 T7; investigación en E2+ |
| 10 | Clasificación de GTIN + GTIN-14 en Silver | P1 | S–M | E2 |
| 11 | Entorno declarado, Makefile, CI | P2 | S | Makefile y lock en E1.6 T1; CI (GitHub Actions con `make verify`) al cerrar E1.6 |
| 12 | Fixtures dorados de payloads reales; DDL en base efímera | P2 | M | E2 / E3 |
| 13 | Fecha de negocio America/Lima | P2 | XS | E1.6 T7 |
| 14 | `inspect_bronze` como gate | P2 | S | E1.6 T9 |
| 15 | Política de rastreo escrita | P2 | S | E1.6 T10 |
| 16 | Dinero en decimal, features mínimas de tokio/parquet, partir adaptadores, errores tipados | P3 | M | después de E2 |
| 17 | Portar desde `worktree/rapid-stone-5ff6`: parser de precios peruanos, `Retry-After` + jitter, dedup con unión en disco, expiración diaria de checkpoints (fecha de Lima) | P2 | S–M | parser de precios, `Retry-After` (tope 30 s), dedup con unión en disco y checkpoints por fecha de Lima reconciliados desde `main`; queda **jitter** en `http.rs` (y `clippy::pedantic` en `make verify`, ≈25 avisos, P3) |

## 9. Decisiones del propietario

| Decisión | Por qué no la toma un agente | Recomendación |
| --- | --- | --- |
| Push de ramas y visibilidad del repo | El repo es público: cada push publica código, `state/` y documentos (incluida la política de rastreo y el User-Agent usado) | Revisar cada rama antes de subirla; valorar si el repo debe seguir siendo público mientras no haya política de rastreo decidida |
| Destino de la rama `worktree/rapid-stone-5ff6` | Es trabajo en curso lanzado por el propietario | Fusionada en `main`; queda decidir si se borra la rama remota `origin/worktree/rapid-stone-5ff6` |
| User-Agent y política de rastreo | Hoy se envía un UA de Chrome sin política escrita; es postura legal y comercial | Revisar términos y `robots.txt` de cada retailer, y decidir si el UA se identifica |
| Regionalización (precio por tienda / código postal) | Cambia alcance, volumen y costo | Investigar cómo fija contexto cada sitio antes de prometer precios por zona |
| Dependencias nuevas (`hypothesis`, extensión `postgres` de DuckDB, Splink, `pytz`) | Amplían superficie y requieren red | Decidir caso a caso en E2/E3 con la justificación de la regla 5 |
| Cadencia de corridas reales (día 2 y siguientes) | Es tráfico contra terceros | Mantener el presupuesto del piloto hasta tener política escrita |

## 10. Protocolo de cada sesión de trabajo

**Al empezar:** `git status` y `git log --oneline -5`; leer esta guía y el plan de la fase; `make verify` para confirmar la línea base.

**Durante:** una tarea a la vez, en el orden del plan; marcar los checkboxes en el plan; si una prueba falla sin causa evidente, `superpowers:systematic-debugging` antes de tocar código.

**Al cerrar:** `make verify` con salida pegada en el reporte; commit por tarea; actualizar el estado en la sección 8; anotar en el reporte QA de la fase lo que quedó sin verificar. No hacer merge a `main`, push ni tags: eso lo decide el propietario tras la re-auditoría.
