# Story E1-01: Modelos Core (1:N), Trait `RetailerAdapter`, Deduplicación y Checkpoints

**Rol BMAD:** PM / SM  
**Prioridad:** P0 (Bloqueante para E1)  
**Metodología:** TDD estricto (`cargo test` rojo -> verde -> refactor)

## Descripción
Definir el núcleo agnóstico de dominio en `rust-ingestor` que separa `Product` de `SkuItem` (relación 1:N), define el contrato asíncrono `RetailerAdapter`, implementa la deduplicación por `payload_hash` (SHA-256) y gestiona checkpoints persistentes en JSON para reanudación de paginación.

## Criterios de Aceptación
1. **Modelo 1:N (`domain.rs`):**
   - `Product` contiene metadatos de producto padre (`product_id`, `raw_name`, `brand`, `category_path`, `raw_description`, `product_url`, `payload_hash`, `raw_payload`, `skus: Vec<SkuItem>`).
   - `Product::to_bronze_rows(is_duplicate_payload: bool)` genera exactamente `N` filas `BronzeSkuRow`, preservando `product_id` y `payload_hash` en todas y omitiendo `raw_payload` cuando `is_duplicate_payload == true`.
2. **Trait `RetailerAdapter` (`adapter.rs`):**
   - Expone `retailer_slug()`, `extractor_version()`, `build_page_url()`, `parse_page_payload()`, y `fetch_page()`.
3. **Deduplicación (`dedup.rs`):**
   - `compute_payload_hash` genera SHA-256 determinístico en hexadecimal.
   - `PayloadDeduplicator` persiste hashes vistos en JSON y devuelve `is_duplicate = true` si el hash ya fue observado en la misma o anterior corrida.
4. **Checkpoints (`checkpoint.rs`):**
   - `CheckpointStore` guarda y recarga `next_offset`, `pages_completed`, `extraction_run_id`, `updated_at`, y `completed` por `(retailer, category)`.

## Tareas Técnicas
- `crates/ingestor/src/domain.rs`
- `crates/ingestor/src/adapter.rs`
- `crates/ingestor/src/dedup.rs`
- `crates/ingestor/src/checkpoint.rs`
