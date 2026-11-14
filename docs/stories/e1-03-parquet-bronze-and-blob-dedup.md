# Story E1-03: Persistencia Parquet Bronze (ZSTD) y Métricas Operativas

**Rol BMAD:** PM / SM  
**Prioridad:** P0  
**Dependencias:** Stories E1-01, E1-02

## Descripción
Persistir las filas `BronzeSkuRow` en Apache Parquet con compresión ZSTD en rutas particionadas por fecha y retailer, y emitir métricas operativas estructuradas al finalizar cada corrida CLI.

## Criterios de Aceptación
1. **Parquet Bronze (`storage/parquet_writer.rs`):**
   - Genera ruta `data/bronze/dt=YYYY-MM-DD/retailer=<name>/part-<run_id>.parquet`.
   - Compresión ZSTD nivel 3 con esquema Arrow estricto.
   - Tests verifican escritura, lectura y preservación de nulos (`raw_payload = None` en duplicados, `ean = None` cuando falte).
2. **Métricas y CLI (`metrics.rs`, `main.rs`):**
   - Flags CLI (`clap`): `--retailer`, `--categories`, `--page-size`, `--max-pages`, `--output-dir`, `--resume`, `--market-id`, `--postal-code`.
   - Emite métricas estructuradas (`tracing` + `metrics-<run_id>.json`): `products_recovered`, `skus_recovered`, `pages_fetched`, `duplicates_detected`, `requests_ok`, `requests_error`, `avg_latency_ms`, `mb_transferred`, `total_duration_secs`, `peak_ram_mb`.
