# Contrato Bronze v2 (`bronze_contract_version = "2.0.0"`)

**Estado:** Vigente en E1.6 · **Versión del contrato:** `2.0.0` (`BRONZE_CONTRACT_VERSION`) · **Formato físico:** Parquet ZSTD nivel 3, particionado Hive `data/bronze/dt=YYYY-MM-DD/retailer=<slug>/part-<extraction_run_id>.parquet`

## Principio rector

Bronze guarda únicamente lo observado en el payload del retailer junto con su procedencia explícita. Ninguna regla de inferencia, sembrado de GTIN, fabricación de stock o conversión de unidades vive en Bronze: toda derivación pertenece a Silver (E2) con `rule_version`.

---

## Esquema Parquet (40 columnas)

Se conservan las 29 columnas originales (índices `0..28`) en su posición, tipo y nulabilidad exactos, y se agregan 11 columnas nuevas al final (índices `29..39`).

### Columnas heredadas (1–29, índices Arrow `0..28`)

| # | Índice | Columna | Tipo Arrow | Null | Descripción |
| --- | ---: | --- | --- | :---: | --- |
| 1 | 0 | `retailer` | `Utf8` | no | `plazavea`, `metro`, `wong`, `tottus` |
| 2 | 1 | `market_id` | `Utf8` | no | Identificador de mercado (ej. `PE`) |
| 3 | 2 | `store_id` | `Utf8` | sí | Tienda o zona aplicada efectivamente en la consulta (ver cambios de semántica) |
| 4 | 3 | `sku_id` | `Utf8` | no | Identificador de SKU en el retailer |
| 5 | 4 | `product_id` | `Utf8` | no | Identificador de producto padre (expansión 1:N) |
| 6 | 5 | `ean` | `Utf8` | sí | Código EAN/GTIN observado (ver cambios de semántica) |
| 7 | 6 | `seller_id` | `Utf8` | sí | Identificador del seller elegido |
| 8 | 7 | `raw_name` | `Utf8` | no | Nombre comercial crudo del SKU o producto |
| 9 | 8 | `brand` | `Utf8` | sí | Marca reportada |
| 10 | 9 | `category_path` | `Utf8` | sí | Ruta jerárquica de categoría reportada |
| 11 | 10 | `raw_description` | `Utf8` | sí | Descripción cruda del producto |
| 12 | 11 | `product_url` | `Utf8` | sí | URL pública del producto |
| 13 | 12 | `image_url` | `Utf8` | sí | URL principal de imagen |
| 14 | 13 | `list_price` | `Float64` | sí | Precio lista / regular |
| 15 | 14 | `selling_price` | `Float64` | sí | Precio de venta online |
| 16 | 15 | `card_price` | `Float64` | sí | Precio con tarjeta/membresía |
| 17 | 16 | `card_program` | `Utf8` | sí | Programa/tarjeta asociada (`Oh!`, `Cencosud`, `Wong Prime`, `CMR`) |
| 18 | 17 | `available_qty` | `Int64` | sí | Cantidad disponible expuesta por el retailer (ver cambios de semántica) |
| 19 | 18 | `currency` | `Utf8` | no | Moneda (`PEN`) |
| 20 | 19 | `sales_channel` | `Utf8` | no | Canal de venta (`ecommerce`) |
| 21 | 20 | `postal_code` | `Utf8` | sí | Código postal aplicado efectivamente en la consulta (ver cambios de semántica) |
| 22 | 21 | `scraped_at` | `Timestamp(Millisecond, "UTC")` | no | Marca temporal UTC de extracción |
| 23 | 22 | `extraction_run_id` | `Utf8` | no | UUID v4 de la corrida |
| 24 | 23 | `extractor_version` | `Utf8` | no | Versión trazable del extractor (ver cambios de semántica) |
| 25 | 24 | `source_endpoint` | `Utf8` | no | URL consultada |
| 26 | 25 | `http_status` | `UInt16` | no | Código HTTP recibido |
| 27 | 26 | `payload_hash` | `Utf8` | no | SHA-256 hex del payload JSON crudo del producto |
| 28 | 27 | `is_duplicate_payload` | `Boolean` | no | `true` si `payload_hash` ya estaba registrado |
| 29 | 28 | `raw_payload` | `Utf8` | sí | JSON crudo del producto (`NULL` si `is_duplicate_payload = true`) |

### Columnas nuevas del Contrato v2 (30–40, índices Arrow `29..39`)

| # | Índice | Columna | Tipo Arrow | Null | Valores / regla |
| --- | ---: | --- | --- | :---: | --- |
| 30 | 29 | `ean_source` | `Utf8` | sí | `item_ean` · `reference_id` · `product_reference` · `spec` (VTEX); `listing` · `pdp` (Tottus). `NULL` ⇔ `ean` es `NULL` |
| 31 | 30 | `availability_status` | `Utf8` | no | `in_stock` · `out_of_stock` · `listed_unknown` · `unknown` |
| 32 | 31 | `availability_source` | `Utf8` | no | `vtex_offer` · `tottus_listing_flag` · `tottus_listing_qty` · `tottus_listing_presence` · `tottus_pdp` · `none` |
| 33 | 32 | `seller_name` | `Utf8` | sí | nombre del seller elegido |
| 34 | 33 | `seller_count` | `Int32` | sí | sellers que ofrecen el SKU en el payload |
| 35 | 34 | `is_default_seller` | `Boolean` | sí | el seller elegido venía marcado `sellerDefault` |
| 36 | 35 | `measurement_unit` | `Utf8` | sí | `measurementUnit` de VTEX, tal cual |
| 37 | 36 | `unit_multiplier` | `Float64` | sí | `unitMultiplier` de VTEX, tal cual (Bronze no multiplica precios) |
| 38 | 37 | `query_term` | `Utf8` | no | término de categoría/búsqueda que produjo la fila |
| 39 | 38 | `content_hash` | `Utf8` | no | SHA-256 del payload sin claves volátiles |
| 40 | 39 | `bronze_contract_version` | `Utf8` | no | `2.0.0` |

---

## Dominios de valores permitidos

### `ean_source` (nullable)

| Valor | Plataforma | Significado |
| --- | --- | --- |
| `item_ean` | VTEX | Tomado de `items[].ean` |
| `reference_id` | VTEX | Tomado de `items[].referenceId[].Value` (validado GS1 Mod-10) |
| `product_reference` | VTEX | Tomado de `productReference` / `productReferenceCode` (solo si el producto tiene exactamente 1 ítem, validado Mod-10) |
| `spec` | VTEX | Tomado de especificaciones de producto (solo si el producto tiene exactamente 1 ítem, validado Mod-10) |
| `listing` | Falabella / Tottus | Observado directamente en el payload de listado |
| `pdp` | Falabella / Tottus | Observado en la PDP consultada en la corrida o en caché verificada con `source = "tottus_pdp_ssr"` |
| `NULL` | Todas | Sin EAN observado (`ean IS NULL`) |

Invariantes:
- `ean IS NULL` ⇔ `ean_source IS NULL`.
- Ningún valor con `seed` puede llegar a `ean_source`.

### `availability_status` (no nullable)

- `in_stock`: el retailer reporta disponibilidad positiva en la fuente observada.
- `out_of_stock`: el retailer reporta explícitamente agotado o cantidad `<= 0`.
- `listed_unknown`: el SKU aparece en el listado sin bandera booleana ni cantidad numérica de stock.
- `unknown`: no hay oferta o señal de disponibilidad interpretable.

### `availability_source` (no nullable)

- `vtex_offer`: derivado de `commertialOffer` (`IsAvailable` y/o `AvailableQuantity`).
- `tottus_listing_flag`: derivado de `isOutOfStock` en listado Tottus.
- `tottus_listing_qty`: derivado de `availableQuantity` o `stock` numérico en listado Tottus.
- `tottus_listing_presence`: presencia en listado Tottus sin campo de stock/disponibilidad.
- `tottus_pdp`: derivado de la PDP Tottus consultada en la corrida actual.
- `none`: sin señal de disponibilidad observada. Únicamente válido cuando `availability_status = "unknown"`.

#### Reglas de extracción (`AvailabilityObs`)

| Caso | qty | status | source |
| --- | --- | --- | --- |
| VTEX `IsAvailable: true` | `AvailableQuantity` crudo | `in_stock` | `vtex_offer` |
| VTEX `IsAvailable: false` | `AvailableQuantity` crudo | `out_of_stock` | `vtex_offer` |
| VTEX sin `IsAvailable`, qty > 0 / qty = 0 | crudo | `in_stock` / `out_of_stock` | `vtex_offer` |
| VTEX sin ninguno de los dos | `None` | `unknown` | `none` |
| Tottus `isOutOfStock: true` (raíz o `availability`) | `Some(0)` | `out_of_stock` | `tottus_listing_flag` |
| Tottus `availableQuantity` o `stock` numérico | ese valor | `in_stock` si > 0, si no `out_of_stock` | `tottus_listing_qty` |
| Tottus listado sin esas señales | `None` | `listed_unknown` | `tottus_listing_presence` |
| Tottus PDP consultada en esta corrida con `in_stock` conocido | qty de la PDP (`Some(0)` si agotado) | `in_stock` / `out_of_stock` | `tottus_pdp` |

`maxOnCartAmount` no es stock y no se lee. La caché nunca aporta disponibilidad a una fila.

### `content_hash` y claves volátiles

`content_hash` se calcula eliminando recursivamente las claves volátiles declaradas por cada adaptador (`RetailerAdapter::volatile_payload_keys()`), serializando el JSON con orden determinista de claves y aplicando SHA-256 en hexadecimal minúscula (si el payload no es JSON válido, coincide con `payload_hash`):
- `VtexAdapter`: `["PriceToken", "PriceValidUntil", "CacheVersionUsedToCallCheckout"]`
- `FalabellaListingAdapter`: `[]`

---

## Cambios de semántica sobre columnas existentes y metadatos de corrida

- **`ean`**: solo valores observados en el payload de esa corrida o en una PDP realmente consultada (o caché verificada `tottus_pdp_ssr`). Las semillas salen de `state/tottus_ean_cache.json` hacia `state/seeds/` como candidatos para evaluar matching en E3.
- **`available_qty`**: solo cantidades que el retailer expone. `99999` de VTEX se conserva tal cual (Silver lo trata como centinela). Nunca se fabrica `1` por mera presencia ni se hereda disponibilidad desde caché.
- **`postal_code` / `store_id`**: `NULL` mientras el adaptador no los aplique de verdad a la consulta (`RetailerAdapter::applies_geo_context() == false`). El valor solicitado queda registrado en el manifiesto de la corrida.
- **`extractor_version`**: `<familia>-v<versión del crate>+<sha7>[-dirty]` (o `+nogit` si no hay repositorio git disponible).
- **Partición `dt`**: fecha de negocio en Lima (`UTC−5` fijo), no fecha UTC.
- **Estado de corrida**: `success` · `truncated` · `partial_error`, con detalle por categoría (`stop_reason`: `exhausted` · `max_pages` · `error`).
