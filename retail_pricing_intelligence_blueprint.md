# Retail Pricing Intelligence (Perú) — Consolidado Técnico, Piloto y Roadmap

**Fecha de corte:** 13 de noviembre de 2026

**Retailers objetivo:** Plaza Vea (`plazavea.com.pe`), Wong (`wong.pe`), Metro (`metro.pe`), Tottus (`tottus.falabella.com.pe`)

**Enfoque:** Data Engineering + Entity Resolution + Retail Analytics + Competitive Pricing Intelligence

## 1. Resultados del Piloto Empírico (Canasta Básica — 5 SKUs)

Extracción y emparejamiento estricto de unidad/gramaje ejecutado el **13 de noviembre de 2026** sobre precios online públicos.

### 1.1. Comparativa de Precios por SKU y Supermercado

| Producto y Presentación | EAN / SKU Ref. | Plaza Vea (S/) | Wong (S/) | Metro (S/) | Tottus (S/) | Mínimo (Cadena) | Dispersión (Máx - Mín %) | Enlaces Verificados | 
| ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | ----- | 
| **Leche Evaporada / Reconstituida Entera Gloria** (Lata 390 g, individual) | PV: `20402646` / Cencosud: `975288` / `975695` / Tottus: `129087940` | **4.20** *(S/ 10.77/kg)* | **4.50** *(S/ 11.54/kg)* | **4.20** *(S/ 10.77/kg)* | **4.20** *(S/ 10.77/kg)* | **S/ 4.20** (Plaza Vea, Metro, Tottus) | **7.14%** | [Plaza Vea](https://www.plazavea.com.pe/leche-reconstituida-entera-gloria-lata-390g-20402646/p) · [Wong](https://www.wong.pe/leche-reconstituida-gloria-lata-390g-975288/p) · [Metro](https://www.metro.pe/leche-evaporada-entera-gloria-lata-390g-975695-2/p) · [Tottus](https://tottus.falabella.com.pe/tottus-pe/product/129087939/leche-gloria-evaporada-entera-390-gr/129087940) | 
| **Aceite Vegetal Primor Clásico** (Botella 900 ml) | PV: `20281566` / Metro: `3201` / Tottus: `113706446` | **9.00** *(S/ 10.00/L)* | `Sin stock / N.D.` | **8.90** *(S/ 9.89/L)* | **8.50** *(S/ 9.44/L)* | **S/ 8.50** (Tottus) | **5.88%** | [Plaza Vea](https://www.plazavea.com.pe/aceite-vegetal-primor-clasico-botella-900ml-20281566/p) · [Wong (Colección)](https://www.wong.pe/colecciones/aceite-vegetal-primor) · [Metro](https://www.metro.pe/aceite-vegetal-primor-cl-sico-botella-900ml-3201-2/p) · [Tottus](https://tottus.falabella.com.pe/tottus-pe/product/113706444/aceite-vegetal-primor-clasico-botella-900-ml/113706446) | 
| **Arroz Extra Costeño** (Bolsa 750 g) | PV: `433778` / Cencosud: `11386` / Tottus: `113707119` | **4.50** *(S/ 6.00/kg)* | **4.50** *(S/ 6.00/kg)* | **4.50** *(S/ 6.00/kg)* | **4.50** *(S/ 6.00/kg)* | **S/ 4.50** (Empate en las 4) | **0.00%** | [Plaza Vea](https://www.plazavea.com.pe/arroz-extra-costeno-bolsa-750g/p) · [Wong](https://www.wong.pe/arroz-extra-coste-o-750g-11386/p) · [Metro](https://www.metro.pe/arroz-extra-costeno-750g-11386/p) · [Tottus](https://tottus.falabella.com.pe/tottus-pe/product/113707117/arroz-extra-costeno-bolsa-750-g/113707119) | 
| **Azúcar Rubia Dulfina** (Bolsa 1 kg) | PV: `20085254` / Cencosud: `333705` / Tottus: `113706893` | **4.80** *(S/ 4.80/kg)* | **4.90** *(S/ 4.90/kg)* | **4.90** *(S/ 4.90/kg)* | `Sin stock / N.D.` | **S/ 4.80** (Plaza Vea) | **2.08%** | [Plaza Vea](https://www.plazavea.com.pe/azucar-rubia-dulfina-bolsa-1-kg/p) · [Wong](https://www.wong.pe/azucar-rubia-dulfina-bolsa-1-kg/p) · [Metro](https://www.metro.pe/azucar-rubia-dulfina-bolsa-1-kg-333705/p) · [Tottus](https://tottus.falabella.com.pe/tottus-pe/product/113706892/azucar-rubia-dulfina-bolsa-1-kg/113706893) | 
| **Avena Instantánea Quaker** (Bolsa / Caja 350 g) | PV: `20045700` / Cencosud: `425642` / Tottus: `131378713` | **9.50** *(S/ 27.14/kg)* | **9.30** *(S/ 26.57/kg)* | **9.40** *(S/ 26.86/kg)* | **9.50** *(S/ 27.14/kg)* | **S/ 9.30** (Wong) | **2.15%** | [Plaza Vea](https://www.plazavea.com.pe/avena-quaker-instantanea-bolsa-350g/p) · [Wong](https://www.wong.pe/avena-instant-nea-quaker-350g-425642/p) · [Metro](https://www.metro.pe/avena-instant-nea-quaker-350g-425642/p) · [Tottus](https://tottus.falabella.com.pe/tottus-pe/product/131378661/avena-quaker-instantanea-caja-350-g/131378713?exp=tottus) | 

**Hallazgos técnicos del piloto:**

* **Diferencia de denominación comercial:** La lata azul de Gloria 390 g figura como *"Leche Reconstituida Entera"* en Plaza Vea/Wong y *"Leche Evaporada Entera"* en Metro/Tottus.

* **Ausencia de presentación exacta:** Ninguna de las 4 cadenas comercializa avena "Clásica/Tradicional" Quaker o 3 Ositos en 300–400 g; el SKU transversal en ese rango es **Avena Instantánea Quaker 350 g**.

* **Faltantes de stock:** En Tottus, tanto [Dulfina 1 kg](https://tottus.falabella.com.pe/tottus-pe/product/113706892/azucar-rubia-dulfina-bolsa-1-kg/113706893) como [Cartavio 1 kg](https://tottus.falabella.com.pe/tottus-pe/product/116364917/azucar-rubia-cartavio-1-kg/116364918) figuran sin stock online. En Wong, la colección pública de [Aceite Primor](https://www.wong.pe/colecciones/aceite-vegetal-primor) solo expuso packs (Tripack) sin ficha individual de 900 ml.

* **Membresía y geolocalización:** Wong aplica **Wong Prime** (-10% en Avena Quaker 350 g: S/ 8.37 vs. S/ 9.30). Tottus resuelve por defecto contra *Cercado de Lima* si no se fija zona/tienda.

### 1.2. Resumen de Costo Total de la Canasta Piloto

| Supermercado | SKUs Encontrados (de 5) | Costo Total Canasta Online (S/) | Diferencia vs. Más Barato (S/ y %) | Observación de Promoción / Tarjeta | 
| ----- | ----- | ----- | ----- | ----- | 
| **Metro** | **5 / 5** | **31.90** | **Base (Más barato 5/5)** | Descuento web directo (-12%) en Aceite Primor Clásico (S/ 8.90 vs. S/ 10.10 regular); sin precio exclusivo Tarjeta Cencosud en estos SKUs. | 
| **Plaza Vea** | **5 / 5** | **32.00** | **+S/ 0.10 (+0.31%)** | Descuento web directo (-7%) en Aceite Primor Clásico (S/ 9.00 vs. S/ 9.70 regular); sin descuento exclusivo Tarjeta Oh! activo en estos SKUs. | 
| **Tottus** | **4 / 5** | **26.70** *(Parcial)* | N.A. *(falta Azúcar 1 kg)* | Descuento web directo (-15%) en Aceite Primor Clásico (S/ 8.50 vs. S/ 10.00 regular); acumula hasta **25 CMR Puntos** en los 4 SKUs. | 
| **Wong** | **4 / 5** | **23.20** *(Parcial)* | N.A. *(falta Aceite 900 ml)* | **Wong Prime** reduce Avena Quaker 350 g a **S/ 8.37** (-10%, subtotal parcial S/ 22.27). | 

## 2. Principios de Diseño y Stack Tecnológico

### 2.1. Cinco Principios Rectores

1. **Separar identidad comercial de equivalencia de comparación:** Distinguir familia de producto (`dim_product_family`), artículo comercial exacto (`dim_trade_item`) y grupo comparable (`dim_comparison_group`).

2. **Inmutabilidad Bronze:** Conservar observaciones originales sin sobrescribir evidencia.

3. **Observación contextualizada:** El precio pertenece a la tupla `(sku, retailer, market_id, store_id, postal_code, sales_channel, scraped_at)` y distingue condiciones promocionales (`regular`, `online`, `tarjeta`, `membresía`, `cupón`, `multipack`).

4. **Indicadores comparables:** Variaciones WoW/MoM mediante pares emparejados (*matched-model*), sin confundir falta de stock con precio cero o descontinuación.

5. **Trazabilidad histórica (SCD Tipo 2):** Versionar reglas de normalización, modelos de embeddings y decisiones de *Entity Resolution*.

### 2.2. Delimitación del Stack

* **Ingesta (`Rust`: `tokio`, `reqwest`, `governor`, `serde`, `arrow-rs`):**

  * Interfaz común (`RetailerAdapter`) con adaptadores independientes por cadena (VTEX para Plaza Vea/Wong/Metro; BFF Falabella para Tottus).

  * Checkpoints de paginación, `extraction_run_id`, `extractor_version`, backoff exponencial y métricas operativas (productos, SKUs, páginas, duplicados, latencia, MB transferidos).

* **Transformación (`Python` + `Polars` + `DuckDB`):**

  * **Polars:** Normalización textual, parser de unidades/multiplicadores, marcas y preparación de features.

  * **DuckDB:** Consultas analíticas sobre Parquet particionado, agregaciones temporales, generación de tablas Gold y escritura directa a PostgreSQL mediante extensión `postgres` (`ATTACH ... TYPE POSTGRES`).

* **Serving (`FastAPI` + `PostgreSQL`):**

  * **PostgreSQL:** Fuente de verdad para catálogo canónico, decisiones de *Entity Resolution*, metadatos de ejecución y agregados Gold materializados para consultas API concurrentes.

## 3. Arquitectura Lakehouse (Medallion) y Modelo Relacional

### 3.1. Capas Parquet (`dt=YYYY-MM-DD/retailer=.../`, compresión ZSTD)

* **Bronze (`raw_catalog` + blob store deduplicado):**

  * Identificación: `retailer`, `market_id`, `store_id`, `sku_id`, `product_id`, `ean`, `seller_id`.

  * Descripción: `raw_name`, `brand`, `category_path`, `raw_description`, `product_url`, `image_url`.

  * Comercial: `list_price`, `selling_price`, `card_price`, `available_qty`, `currency`, `sales_channel`.

  * Contexto: `postal_code`, `scraped_at`, `extraction_run_id`, `extractor_version`, `source_endpoint`, `http_status`, `payload_hash`.

  * *Ajuste V1:* Deduplicar `raw_payload` por `payload_hash` para evitar almacenar JSON repetido diariamente sin cambios.

* **Silver (`clean_skus`):**

  * Identidad/clasificación normalizada, características físicas (`net_qty`, `net_qty_unit`, `pack_units`, `total_content_base`, `product_form`, `flavor_variant`), comerciales (`promotion_type`, `promotion_eligibility`, `in_stock`) y calculadas (`unit_price_std`, `comparison_unit`, `is_multipack`, `normalization_status`).

  * Unidades soportadas progresivamente: `g`, `kg`, `ml`, `L`, `unidad`, `metro`, `rollo`, `paquete`, `hoja`, `dosis` (sin convertir `g ↔ ml` sin densidad; distinguiendo peso neto vs. escurrido).

* **Gold (Tablas Analíticas):**

  * `fact_price_observation` (granularidad intradía por contexto).

  * `fact_price_daily` (consolidado diario).

  * `agg_competitive_position`, `agg_promotion_performance`, `agg_assortment`, `agg_price_dispersion`, `agg_market_basket`.

### 3.2. Esquema PostgreSQL (Catálogo Maestro y Auditoría)

* `dim_product_family(family_id, brand, product_type, category_id, normalized_name)`

* `dim_trade_item(trade_item_id, family_id, gtin, brand, commercial_description, net_quantity, net_quantity_unit, pack_units, presentation, product_form)`

* `dim_comparison_group(comparison_group_id, category_id, comparison_unit, comparability_rule, group_description)`

* `bridge_sku_trade_item(retailer, market_id, sku_id, trade_item_id, match_method, confidence_score, match_status, model_version, rule_version, valid_from, valid_to, verified_at)`

* `match_review_log(review_id, candidate_a, candidate_b, suggested_match, final_decision, reviewer, decision_reason, model_version, reviewed_at)`

## 4. Entity Resolution y Módulos Analíticos

### 4.1. Cascada de Matching

1. **Nivel 1 (Determinístico GTIN/EAN):** Validación de formato, longitud, dígito de control y consistencia con nivel de empaquetado (unidad vs. caja/pack).

2. **Nivel 2 (Parser estructural versionado):** Extracción determinística de marca, tipo, cantidad, unidad, multiplicador y variante.

3. **Nivel 3 (Embeddings + Active Learning):** `paraphrase-multilingual-MiniLM-L12-v2` únicamente para recuperación de candidatos tras *blocking* por categoría/cantidad. Calibración de umbrales sobre dataset etiquetado (Precision, Recall, F1, FPR, Coverage) con clase explícita de **abstención** y cola de revisión humana.

### 4.2. Seis Módulos de Retail Analytics

1. **Competitive Price Monitoring:** `price_gap = (precio_retailer / mediana_benchmark - 1) * 100`, mín/máx/mediana, variaciones diarias/semanales/mensuales.

2. **Pricing Strategy:** Posicionamiento relativo por categoría/marca, frecuencia y volatilidad de cambios de precio.

3. **Promotion Intelligence:** `promotion_depth = (regular_price - promotional_price) / regular_price`, frecuencia, duración, separación por medio de pago/membresía.

4. **Assortment Intelligence:** Profundidad por categoría, marcas propias, exclusivos, solapamiento, altas y regla temporal para distinguir quiebre de stock vs. retiro.

5. **Market Basket Intelligence:** Índice tipo Laspeyres `Index_t = SUM(w_i_0 * P_i_t / P_i_0) * 100` sobre canastas propias (no réplica del IPC INEI).

6. **Price Dispersion Intelligence:** Rango, CV y diferencial Máx-Mín únicamente sobre conjuntos comparables y contextos compatibles.

## 5. Roadmap y Ajustes Prácticos V1

* **E1 (Ingesta y conectores):** Arrancar con **2 conectores reales** (Plaza Vea VTEX + Tottus Listing) y **1–2 zonas de control** (Lima Centro + Trujillo) antes de escalar a multizona. Medir latencia, duplicados y cobertura real de `items[].ean`.

* **E2 (Normalización Silver):** Cubrir primero unidades core (`g/kg`, `ml/L`, `unidad/rollo/paquete`) en Abarrotes, Lácteos, Bebidas y Limpieza básica; marcar cola larga como `pending_rule`.

* **E3 (Entity Resolution):** Construir benchmark etiquetado (\~300 pares positivos y *hard negatives* pack vs. unidad) y cola de revisión ligera antes de entrenar clasificadores complejos.

* **E4 (Gold & Retail Analytics):** Consolidar series históricas y materializar agregados en PostgreSQL vía DuckDB.

* **E5 (API Serving):** FastAPI leyendo agregados materializados en PostgreSQL.

* **E6 (Retail Pricing Intelligence):** Paneles, alertas y reportes por categoría/retailer.

## 6. Prompt para Antigravity CLI (`agy`)

```text
Actúa como un Senior Data Engineer (Rust + Python/Polars/DuckDB + PostgreSQL) y orquestador técnico.
Vamos a inicializar el repositorio `retail-intel-pe` en el directorio actual siguiendo estrictamente la arquitectura Medallion y los principios de diseño del proyecto Retail Pricing Intelligence (Perú: Plaza Vea, Wong, Metro, Tottus).

METODOLOGÍA, SKILLS Y AGENTES OBLIGATORIOS:
Apóyate explícitamente en **bmad-method**, **superpowers** y los **skills y subagentes especializados** disponibles en tu entorno:
1. **BMAD-METHOD (`bmad-method`):**
   - Activa el flujo ágil estructurado con los roles **Architect**, **PM/SM**, **Dev** y **QA**.
   - Genera primero especificaciones concisas para la fase (`docs/architecture-e1.md` y stories atómicas en `docs/stories/e1-*.md`) antes de escribir código de producción.
2. **Superpowers (`superpowers`):**
   - Usa `writing-plans` para descomponer E1 en tareas pequeñas y verificables.
   - Usa `dispatching-parallel-agents` / `subagent-driven-development` para paralelizar módulos independientes (`RustIngestorAgent` para `VtexAdapter` y `FalabellaListingAdapter`, `DataSchemaAgent` para PostgreSQL DDL + DuckDB, y `QABenchmarkAgent` para pruebas y auditoría).
   - Aplica `test-driven-development` (TDD) en el trait `RetailerAdapter`, deduplicación por `payload_hash` y lógica de checkpoints antes de implementar.
   - Usa `systematic-debugging` ante errores de paginación, esquemas JSON cambiantes o bloqueos HTTP/WAF.
   - Ejecuta `verification-before-completion` y `requesting-code-review` antes de cerrar cualquier story o reportar éxito.
3. **Skills locales:** Carga y aplica los skills de Rust, Data Engineering (DuckDB/Parquet/Polars) y PostgreSQL si están disponibles en tu registro.

ALCANCE DE ESTA SESIÓN (SOLO FASE E1 — INGESTA, BRONZE Y BENCHMARK):
No implementes aún reglas completas de E2 ni embeddings de E3. Concéntrate en entregar un E1 reproducible, observable y probado:

1. Estructura del proyecto:
   - `docs/` (especificación breve BMAD de E1 y stories).
   - `rust-ingestor/` (Cargo workspace con `tokio`, `reqwest`, `governor`, `serde`, `serde_json`, `arrow`, `parquet`, `chrono`, `sha2`, `uuid`, `tracing`, `clap`).
   - `sql/postgres_schema.sql` (DDL inicial para `extraction_runs` + las 5 tablas del catálogo maestro: `dim_product_family`, `dim_trade_item`, `dim_comparison_group`, `bridge_sku_trade_item`, `match_review_log`).
   - `scripts/inspect_bronze.py` (script mínimo con DuckDB para auditar los Parquet generados: conteo de productos vs SKUs, cobertura real de EAN no nulos, tasa de duplicados y distribución de precios).

2. Diseño del `rust-ingestor`:
   - Define un trait común `RetailerAdapter` que separe `Product` de `SkuItem` (1:N).
   - Implementa 2 adaptadores funcionales iniciales:
     a) `VtexAdapter` (configurable por retailer: Plaza Vea, Metro, Wong) con paginación `_from`/`_to`, checkpoints locales en JSON/SQLite para reanudación, `extraction_run_id` (UUID), `extractor_version`, `payload_hash` (SHA-256), backoff exponencial con límite de reintentos y rate limiting con `governor` (máx. 2 req/s por dominio).
     b) `FalabellaListingAdapter` (Tottus) respetando la misma interfaz `RetailerAdapter`.
   - Persistencia Bronze en Parquet con compresión ZSTD particionada por `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet`, deduplicando `raw_payload` por `payload_hash`.
   - Emite métricas estructuradas al finalizar cada corrida: productos recuperados, SKUs recuperados, páginas consultadas, duplicados detectados, requests OK/error, latencia promedio, MB transferidos, tiempo total y pico de RAM.

3. Validación obligatoria antes de terminar (`verification-before-completion`):
   - Ejecuta `cargo test` y `cargo build --release`.
   - Ejecuta una corrida piloto acotada (ej. 2 categorías de abarrotes/lácteos en Plaza Vea y Metro) contra `data/bronze/`.
   - Corre `scripts/inspect_bronze.py` sobre el Parquet generado y muéstrame las métricas reales obtenidas, la revisión de calidad del agente QA y cualquier error o bloqueo HTTP encontrado.
```
