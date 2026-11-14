# Story E1-02: Adaptadores `VtexAdapter` y `FalabellaListingAdapter`

**Rol BMAD:** PM / SM  
**Prioridad:** P0  
**Dependencias:** Story E1-01

## Descripción
Implementar los dos adaptadores iniciales bajo el trait `RetailerAdapter`, incorporando rate limiting por dominio con `governor` (máximo 2 req/s) y backoff exponencial con límite de reintentos.

## Criterios de Aceptación
1. **`VtexAdapter` (`adapters/vtex.rs`):**
   - Configurable por retailer VTEX: `plazavea` (`www.plazavea.com.pe`), `metro` (`www.metro.pe`), `wong` (`www.wong.pe`).
   - Paginación `_from` y `_to` (`_from = offset`, `_to = offset + page_size - 1`).
   - Parsea correctamente productos multi-SKU (`items[]`), normaliza `ean` vacío a `None`, extrae `ListPrice`, `Price`, `AvailableQuantity` y detecta `card_price` cuando existan promociones/teasers aplicables.
   - Aplica rate limit `<= 2 req/s` con `governor` y retry con backoff exponencial hasta 3 intentos en `429`/`5xx`.
2. **`FalabellaListingAdapter` (`adapters/falabella.rs`):**
   - Configurado para `tottus` (`https://tottus.falabella.com.pe/tottus-pe/search?Ntt=<query>&page=<page>`).
   - Soporta tanto respuesta JSON directa como extracción de `<script id="__NEXT_DATA__" type="application/json">...</script>` desde HTML.
   - Mapea `normalPrice` -> `list_price`, `internetPrice`/`eventPrice` -> `selling_price`, y `cmrPrice` -> `card_price`.
   - Aplica rate limit `<= 2 req/s` con `governor` y retry con backoff exponencial.

## Tareas Técnicas
- `crates/ingestor/src/http.rs` (cliente HTTP compartido con `governor` y backoff exponencial)
- `crates/ingestor/src/adapters/vtex.rs`
- `crates/ingestor/src/adapters/falabella.rs`
