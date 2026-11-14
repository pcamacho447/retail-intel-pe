# Política de Rastreo Operativa (`docs/crawl-policy.md`)

**Estado:** Vigente para piloto controlado E1.6 · **Alcance:** Ingestor Bronze (`rust-ingestor`, contrato `2.0.0`) · **Retailers cubiertos:** `plazavea`, `metro`, `wong`, `tottus`

Este documento describe exclusivamente los controles de acceso HTTP, límites de tasa y reglas operativas verificadas en el código fuente del ingestor, junto con las decisiones de gobierno pendientes de aprobación por parte del propietario antes de habilitar corridas periódicas o de catálogo completo.

---

## 1. Superficie de Endpoints y Alcance de Acceso

El ingestor accede **únicamente a endpoints públicos de catálogo/búsqueda y páginas públicas de detalle de producto (PDP)**. No utiliza inicio de sesión, cuentas de usuario, cookies de sesión autenticada ni bypass de controles de acceso.

| Retailer | Adaptador | Base URL verificada | Endpoints consultados | Autenticación / Login |
| :--- | :--- | :--- | :--- | :---: |
| `plazavea` | `VtexAdapter` (`vtex.rs:31`) | `https://www.plazavea.com.pe` | `GET /api/catalog_system/pub/products/search/{categoria}?_from={offset}&_to={to}` (`vtex.rs:633-636`) | No |
| `metro` | `VtexAdapter` (`vtex.rs:32`) | `https://www.metro.pe` | `GET /api/catalog_system/pub/products/search/{categoria}?_from={offset}&_to={to}` (`vtex.rs:633-636`) | No |
| `wong` | `VtexAdapter` (`vtex.rs:33`) | `https://www.wong.pe` | `GET /api/catalog_system/pub/products/search/{categoria}?_from={offset}&_to={to}` (`vtex.rs:633-636`) | No |
| `tottus` | `FalabellaListingAdapter` (`falabella.rs:19`) | `https://tottus.falabella.com.pe` | Listado HTML/SSR `GET /tottus-pe/search?Ntt={termino}&page={page}` (o `GET {ruta}?page={page}` si inicia con `/`, `falabella.rs:820-829`) + PDP pública `GET {product_url}` solo cuando `--enrich-tottus-pdp` está activo (`falabella.rs:864-875`) | No |

### Reglas de extracción asociadas al tráfico

- **Tamaño de página VTEX:** `VtexAdapter::build_page_url` y `fetch_page` acotan `page_size` con `page_size.clamp(1, 50)` (`vtex.rs:623, 650`).
- **Enriquecimiento PDP en Tottus (`--enrich-tottus-pdp`):** Desactivado por defecto en la CLI (`main.rs:69-70`). Cuando se habilita, solo consulta la PDP de SKUs cuyo `ean` en el listado es `None` y que no poseen entrada previa en la caché (`state/tottus_ean_cache.json` por defecto, `main.rs:72-73`) con `pdp_checked == true` y `source == "tottus_pdp_ssr"` (`falabella.rs:214-224`).
- **Contexto geográfico (`--postal-code`):** Ningún adaptador implementa aún contexto geográfico real (`RetailerAdapter::applies_geo_context()` retorna `false` en `adapter.rs:83-85`). Por diseño (`runner.rs:53-65`), `--postal-code` no modifica las peticiones HTTP enviadas a los retailers y se registra `NULL` en Bronze, trazando el valor solicitado únicamente en `metrics-*.json`.

---

## 2. Límites de Tasa, Reintentos y Tiempos de Espera

Todos los controles HTTP están centralizados en `RateLimitedHttpClient` (`rust-ingestor/crates/ingestor/src/http.rs`):

| Parámetro | Valor implementado | Referencia exacta en código |
| :--- | :--- | :--- |
| **Tasa máxima de peticiones** | **`2 req/s`** (`NonZeroU32::new(2)`) | `http.rs:39, 71` |
| **Ráfaga permitida (burst)** | **`1`** (`allow_burst(NonZeroU32::new(1))`) | `http.rs:72` |
| **Concurrencia HTTP** | Secuencial por corrida CLI (`Arc<RateLimitedHttpClient>` único compartido entre listado y PDPs, `await` por petición sobre `limiter.until_ready()`) | `main.rs:150`, `http.rs:87`, `falabella.rs:869-870` |
| **Timeout por petición** | **`20 s`** (`Duration::from_secs(20)`) | `http.rs:42, 65` |
| **Reintentos máximos** | **`3` reintentos** (hasta 4 intentos totales por URL) | `http.rs:40, 86` |
| **Política de backoff** | Exponencial **`500 ms * 2^attempt`** (`500 ms`, `1000 ms`, `2000 ms`) | `http.rs:41, 178-190` |
| **Códigos HTTP exitosos** | Cualquier `2xx` (`resp.status().is_success()`, incluyendo `200` y `206 Partial Content` de VTEX) | `http.rs:93` |
| **Condiciones reintentables** | HTTP `429`, HTTP `5xx` (`500..=599`), errores de transporte y errores de lectura del cuerpo | `http.rs:111-145, 160-171` |
| **Condiciones NO reintentables** | Otros códigos `4xx` (p. ej. `403`, `404`), que abortan inmediatamente sin reintento | `http.rs:146-158` |
| **Cabeceras por defecto** | `Accept: application/json, text/html, application/xhtml+xml;q=0.9, */*;q=0.8`; `Accept-Language: es-PE,es;q=0.9,en;q=0.8` | `http.rs:53-62` |
| **User-Agent actual** | `Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36` | `http.rs:66` |

### Manejo de errores durante una corrida

- Si falla `fetch_page` (catálogo/listado), el bucle de esa categoría se detiene con `stop_reason = "error"` (`runner.rs:189-200`), persiste las páginas previas válidas y finaliza el manifiesto con `status = "partial_error"` si hubo otras páginas previas exitosas (`runner.rs:237-239`), o retorna el error directamente si `pages_fetched == 0` (`runner.rs:221-226`).
- Si falla una PDP individual de Tottus bajo `--enrich-tottus-pdp`, el adaptador registra una advertencia (`tracing::warn!`) y continúa el lote de listado (`falabella.rs:876-884`).
- **Regla operativa ante bloqueos en piloto:** ante cualquier HTTP `403`, HTTP `429` o error HTTP repetido en pruebas manuales o piloto, la ejecución debe detenerse de inmediato sin modificar flags ni forzar reintentos adicionales.

---

## 3. Presupuesto de Peticiones del Piloto E1.6 (`<= 65` requests)

Para validar el contrato Bronze v2 sin generar carga indebida sobre los sitios destino, el piloto E1.6 está estrictamente acotado a **un máximo total de `65` requests HTTP** distribuidos exactamente en las 4 ejecuciones siguientes:

| Retailer | Comando piloto (parámetros) | Páginas máximas | Presupuesto máximo de requests |
| :--- | :--- | :---: | :--- |
| `plazavea` | `--categories lacteos,abarrotes --page-size 20 --max-pages 2` | 4 | `4` (listado VTEX) |
| `metro` | `--categories lacteos,abarrotes,televisor --page-size 20 --max-pages 2` | 6 | `6` (listado VTEX) |
| `wong` | `--categories lacteos,abarrotes,televisor --page-size 20 --max-pages 2` | 6 | `6` (listado VTEX) |
| `tottus` | `--categories leche --max-pages 1 --enrich-tottus-pdp` | 1 | `1` listado + hasta `48` PDPs (solo SKUs sin EAN en listado ni entrada `tottus_pdp_ssr` previa en `state/tottus_ean_cache.json`) = hasta `49` |
| **TOTAL PILOTO** | **4 corridas** | **17 páginas** | **`<= 65` requests HTTP totales** (`16` VTEX + `<= 49` Tottus) |

*(Nota: fuera de estos parámetros acotados, los valores por defecto en `main.rs:50-54` son `--page-size 20` y `--max-pages 2`.)*

---

## 4. Decisiones pendientes del propietario

Las siguientes definiciones son de carácter legal, comercial y de gobierno operativo. **No deben ser decididas ni modificadas unilateralmente por agentes técnicos** y quedan pendientes de resolución por el propietario antes de habilitar corridas periódicas (día 2 o posteriores) o escalar la cobertura:

1. **Identificación del `User-Agent`:**
   - **Situación actual:** `rust-ingestor/crates/ingestor/src/http.rs:66` envía un `User-Agent` estándar de navegador Chrome (`Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36`). En esta tarea (E1.6) no se modifica dicho valor ni los límites de tasa.
   - **Decisión requerida:** definir si se conserva el UA actual o si el ingestor debe identificarse explícitamente con un `User-Agent` propio del proyecto y canal de contacto técnico, evaluando el equilibrio entre transparencia operativa y posibles bloqueos WAF.

2. **Revisión de Términos de Servicio (ToS) y `robots.txt` por retailer:**
   - **Situación actual:** no se ha realizado revisión legal de los Términos de Servicio ni análisis formal de directivas `robots.txt` para `www.plazavea.com.pe`, `www.metro.pe`, `www.wong.pe` y `tottus.falabella.com.pe`.
   - **Decisión requerida:** revisar y aprobar el marco de cumplimiento por cada dominio antes de autorizar cualquier recolección recurrente o ampliación de categorías/páginas.

3. **Ventanas horarias y cadencia de ejecución:**
   - **Situación actual:** las ejecuciones son exclusivamente manuales y acotadas al presupuesto del piloto (`<= 65` requests a `<= 2 req/s`). Incluso la medición de repetibilidad de día 2 requiere visto bueno explícito del propietario.
   - **Decisión requerida:** establecer ventanas horarias permitidas (idealmente horarios de baja demanda en hora local de Lima `UTC-5`), cadencia máxima (p. ej. 1 corrida diaria o semanal), presupuesto máximo de requests por dominio/corrida y política ante cambios en precios regionalizados (`applies_geo_context`).
