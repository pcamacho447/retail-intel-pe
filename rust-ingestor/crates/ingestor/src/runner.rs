use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use chrono::{DateTime, FixedOffset, NaiveDate, TimeDelta, Utc};
use uuid::Uuid;

use crate::adapter::{FetchContext, RetailerAdapter};
use crate::checkpoint::{CheckpointState, CheckpointStore};
use crate::dedup::PayloadDeduplicator;
use crate::domain::{BronzeSkuRow, BRONZE_CONTRACT_VERSION};
use crate::http::RateLimitedHttpClient;
use crate::metrics::{read_peak_ram_mb, CategoryOutcome, RunMetrics};
use crate::storage::parquet_writer::BronzeWriter;

const LIMA_UTC_OFFSET_SECS: i32 = 5 * 3600;

/// Returns the Peru business date (`UTC-5` fixed, no DST) for the given UTC instant.
pub fn business_date_lima(now: DateTime<Utc>) -> NaiveDate {
    match FixedOffset::west_opt(LIMA_UTC_OFFSET_SECS) {
        Some(lima_offset) => now.with_timezone(&lima_offset).date_naive(),
        None => (now - TimeDelta::hours(5)).date_naive(),
    }
}

#[derive(Debug, Clone)]
pub struct IngestConfig {
    pub retailer: String,
    pub categories: Vec<String>,
    pub page_size: usize,
    pub max_pages: usize,
    pub output_dir: PathBuf,
    pub resume: bool,
    pub market_id: String,
    pub postal_code: Option<String>,
}

pub async fn run_ingest(
    adapter: &dyn RetailerAdapter,
    http: &RateLimitedHttpClient,
    config: &IngestConfig,
) -> anyhow::Result<RunMetrics> {
    let started_at = Instant::now();
    let started_utc = Utc::now();
    let extraction_run_id = Uuid::new_v4();
    let run_date = business_date_lima(started_utc);
    let business_date = run_date.format("%Y-%m-%d").to_string();
    let slug = adapter.retailer_slug();

    let mut dedup = PayloadDeduplicator::open(&config.output_dir)?;
    let checkpoints = CheckpointStore::new(&config.output_dir);

    let requested_postal_code = config.postal_code.clone();
    let applied_postal_code = if adapter.applies_geo_context() {
        requested_postal_code.clone()
    } else {
        if let Some(postal_code) = requested_postal_code.as_deref() {
            tracing::warn!(
                retailer = slug,
                requested_postal_code = %postal_code,
                "Requested postal_code was not applied because adapter does not support geo context"
            );
        }
        None
    };

    let base_ctx = FetchContext::for_run(
        extraction_run_id,
        config.market_id.clone(),
        applied_postal_code,
    );

    let mut writer = BronzeWriter::create(&config.output_dir, run_date, slug, extraction_run_id)?;
    let mut pending_checkpoints: Vec<CheckpointState> = Vec::new();
    let mut categories_detail: Vec<CategoryOutcome> = Vec::with_capacity(config.categories.len());
    let mut products_recovered: usize = 0;
    let mut skus_recovered: usize = 0;
    let mut pages_fetched: usize = 0;
    let mut duplicates_detected: usize = 0;
    let mut total_bytes: usize = 0;
    let mut latencies_ms: Vec<f64> = Vec::new();
    let mut last_error: Option<anyhow::Error> = None;

    for category in &config.categories {
        let mut offset = 0usize;
        let mut pages_completed_total = 0usize;
        let mut effective_page_size = config.page_size;

        if config.resume {
            if let Some(cp) = checkpoints.load(slug, category)? {
                if cp.completed {
                    tracing::info!(
                        retailer = slug,
                        category = %category,
                        "Skipping already completed category from checkpoint"
                    );
                    categories_detail.push(CategoryOutcome {
                        category: category.clone(),
                        pages_fetched: 0,
                        products: 0,
                        skus: 0,
                        products_rejected: 0,
                        products_without_skus: 0,
                        next_offset: cp.next_offset,
                        stop_reason: "exhausted".to_string(),
                    });
                    continue;
                }
                offset = cp.next_offset;
                pages_completed_total = cp.pages_completed;
                if let Some(saved_ps) = cp.page_size.filter(|&ps| ps > 0) {
                    effective_page_size = saved_ps;
                }
                tracing::info!(
                    retailer = slug,
                    category = %category,
                    offset = offset,
                    pages_completed = pages_completed_total,
                    page_size = effective_page_size,
                    "Resuming category from checkpoint"
                );
            }
        }

        let category_ctx = base_ctx.clone().with_query_term(category);
        let mut pages_this_run = 0usize;
        let mut cat_products = 0usize;
        let mut cat_skus = 0usize;
        let mut cat_products_rejected = 0usize;
        let mut cat_products_without_skus = 0usize;
        let mut cat_next_offset = offset;
        let mut cat_stop_reason: Option<&'static str> = None;
        let mut latest_cp: Option<CheckpointState> = None;

        while pages_this_run < config.max_pages {
            match adapter
                .fetch_page(category, offset, effective_page_size, &category_ctx)
                .await
            {
                Ok(batch) => {
                    pages_fetched += 1;
                    pages_completed_total += 1;
                    pages_this_run += 1;
                    total_bytes += batch.raw_bytes;
                    latencies_ms.push(batch.latency_ms);
                    cat_products_rejected += batch.products_rejected;
                    cat_next_offset = batch.next_offset;

                    let mut page_rows: Vec<BronzeSkuRow> = Vec::new();
                    for product in &batch.products {
                        products_recovered += 1;
                        cat_products += 1;
                        if product.skus.is_empty() {
                            cat_products_without_skus += 1;
                        }
                        let is_dup = dedup.check_and_mark(&product.payload_hash);
                        if is_dup {
                            duplicates_detected += 1;
                        }
                        let bronze_rows = product.to_bronze_rows(is_dup);
                        skus_recovered += bronze_rows.len();
                        cat_skus += bronze_rows.len();
                        page_rows.extend(bronze_rows);
                    }

                    if let Err(err) = writer.write_rows(&page_rows) {
                        writer.abort();
                        return Err(err);
                    }

                    let completed = !batch.has_more;
                    latest_cp = Some(CheckpointState {
                        retailer: slug.to_string(),
                        category: category.clone(),
                        next_offset: batch.next_offset,
                        pages_completed: pages_completed_total,
                        page_size: Some(effective_page_size),
                        extraction_run_id,
                        updated_at: Utc::now(),
                        completed,
                    });

                    if completed {
                        cat_stop_reason = Some("exhausted");
                        break;
                    }
                    offset = batch.next_offset;
                }
                Err(err) => {
                    tracing::error!(
                        retailer = slug,
                        category = %category,
                        offset = offset,
                        error = %format!("{err:#}"),
                        "Failed fetching page"
                    );
                    last_error = Some(err);
                    cat_stop_reason = Some("error");
                    break;
                }
            }
        }

        if let Some(cp) = latest_cp {
            pending_checkpoints.push(cp);
        }

        let stop_reason = cat_stop_reason.unwrap_or("max_pages");
        categories_detail.push(CategoryOutcome {
            category: category.clone(),
            pages_fetched: pages_this_run,
            products: cat_products,
            skus: cat_skus,
            products_rejected: cat_products_rejected,
            products_without_skus: cat_products_without_skus,
            next_offset: cat_next_offset,
            stop_reason: stop_reason.to_string(),
        });
    }

    if pages_fetched == 0 {
        if let Some(err) = last_error {
            writer.abort();
            return Err(err);
        }
    }

    // Finalize Parquet first so dedup hashes and checkpoints are only committed once Bronze data is durable.
    let parquet_path = writer.finish()?;
    dedup.persist()?;
    for cp in &pending_checkpoints {
        checkpoints.save(cp)?;
    }

    let http_stats = http.stats();
    let error_msg = last_error.as_ref().map(|err| format!("{err:#}"));
    let status =
        if categories_detail.iter().any(|c| c.stop_reason == "error") || error_msg.is_some() {
            "partial_error"
        } else if categories_detail
            .iter()
            .any(|c| c.stop_reason == "max_pages")
        {
            "truncated"
        } else {
            "success"
        };

    let metrics = RunMetrics {
        extraction_run_id,
        retailer: slug.to_string(),
        extractor_version: adapter.extractor_version().to_string(),
        bronze_contract_version: BRONZE_CONTRACT_VERSION.to_string(),
        categories: config.categories.clone(),
        categories_detail,
        started_at: started_utc,
        finished_at: Utc::now(),
        business_date,
        requested_postal_code,
        status: status.to_string(),
        error: error_msg,
        products_recovered,
        skus_recovered,
        pages_fetched,
        duplicates_detected,
        requests_ok: http_stats.requests_ok,
        requests_error: http_stats.requests_error,
        avg_latency_ms: RunMetrics::compute_avg_latency_ms(&latencies_ms),
        mb_transferred: RunMetrics::bytes_to_mb(total_bytes),
        total_duration_secs: started_at.elapsed().as_secs_f64(),
        peak_ram_mb: read_peak_ram_mb(),
        parquet_path: parquet_path.display().to_string(),
    };

    let metrics_dir = parquet_path
        .parent()
        .context("Parquet path must have a parent directory")?;
    let metrics_path = metrics.write_json(metrics_dir)?;

    tracing::info!(
        extraction_run_id = %metrics.extraction_run_id,
        retailer = %metrics.retailer,
        extractor_version = %metrics.extractor_version,
        business_date = %metrics.business_date,
        requested_postal_code = ?metrics.requested_postal_code,
        status = %metrics.status,
        products_recovered = metrics.products_recovered,
        skus_recovered = metrics.skus_recovered,
        pages_fetched = metrics.pages_fetched,
        duplicates_detected = metrics.duplicates_detected,
        requests_ok = metrics.requests_ok,
        requests_error = metrics.requests_error,
        avg_latency_ms = metrics.avg_latency_ms,
        mb_transferred = metrics.mb_transferred,
        total_duration_secs = metrics.total_duration_secs,
        peak_ram_mb = metrics.peak_ram_mb,
        parquet_path = %metrics.parquet_path,
        metrics_path = %metrics_path.display(),
        "Ingestion run finished"
    );

    if let Some(err) = last_error {
        return Err(err);
    }

    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::vtex::{VtexAdapter, VtexRetailer};
    use crate::checkpoint::{CheckpointState, CheckpointStore};
    use arrow::array::{Array, BooleanArray, StringArray};
    use chrono::TimeZone;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::fs::File;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    async fn spawn_vtex_sequence_with_requests(
        pages: Vec<(u16, String)>,
    ) -> (String, Arc<AtomicUsize>, Arc<std::sync::Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let pages = Arc::new(pages);
        let calls = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));

        let calls_ref = calls.clone();
        let requests_ref = requests.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let pages = pages.clone();
                let calls = calls_ref.clone();
                let requests = requests_ref.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req_line = String::from_utf8_lossy(&buf[..n])
                        .lines()
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    requests.lock().unwrap().push(req_line);
                    let idx = calls.fetch_add(1, Ordering::SeqCst);
                    let (status, body) = pages.get(idx).cloned().unwrap_or((200, "[]".to_string()));
                    let reason = if status == 206 {
                        "Partial Content"
                    } else {
                        "OK"
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        (format!("http://{addr}"), calls, requests)
    }

    async fn spawn_vtex_sequence(pages: Vec<(u16, String)>) -> (String, Arc<AtomicUsize>) {
        let (base_url, calls, _) = spawn_vtex_sequence_with_requests(pages).await;
        (base_url, calls)
    }

    fn fast_http() -> Arc<RateLimitedHttpClient> {
        Arc::new(
            RateLimitedHttpClient::with_config(
                NonZeroU32::new(50).unwrap(),
                1,
                5,
                Duration::from_secs(5),
            )
            .unwrap(),
        )
    }

    #[tokio::test]
    async fn run_ingest_deduplicates_across_runs_and_writes_parquet_and_metrics() {
        let dir = TempDir::new().unwrap();
        let prod_json = r#"[{"productId":"20402646","productName":"Leche Gloria","brand":"Gloria","items":[{"itemId":"1001","ean":"7750243012345","name":"Leche Gloria 390g","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":4.5,"Price":4.2,"AvailableQuantity":20}}]}]}]"#;

        let (base_url, _) = spawn_vtex_sequence(vec![
            (206, prod_json.to_string()),
            (206, prod_json.to_string()),
        ])
        .await;

        let http1 = fast_http();
        let adapter1 = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http1.clone());
        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 20,
            max_pages: 2,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: Some("150101".to_string()),
        };

        // Run 1: new payload
        let m1 = run_ingest(&adapter1, &http1, &cfg).await.unwrap();
        assert_eq!(m1.products_recovered, 1);
        assert_eq!(m1.skus_recovered, 1);
        assert_eq!(m1.duplicates_detected, 0);
        assert_eq!(m1.pages_fetched, 1);
        assert!(PathBuf::from(&m1.parquet_path).exists());

        let cp = CheckpointStore::new(dir.path())
            .load("plazavea", "lacteos")
            .unwrap()
            .unwrap();
        assert!(cp.completed);
        assert_eq!(cp.pages_completed, 1);

        // Run 2: same payload -> duplicate detected and raw_payload omitted in Parquet
        let http2 = fast_http();
        let adapter2 = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http2.clone());
        let m2 = run_ingest(&adapter2, &http2, &cfg).await.unwrap();
        assert_eq!(m2.products_recovered, 1);
        assert_eq!(m2.skus_recovered, 1);
        assert_eq!(m2.duplicates_detected, 1);

        let file2 = File::open(&m2.parquet_path).unwrap();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(file2)
            .unwrap()
            .build()
            .unwrap();
        let batch = reader.next().unwrap().unwrap();
        let dup_col = batch
            .column_by_name("is_duplicate_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(dup_col.value(0));

        let raw_col = batch
            .column_by_name("raw_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(raw_col.is_null(0));

        let query_term_col = batch
            .column_by_name("query_term")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(query_term_col.value(0), "lacteos");

        let contract_ver_col = batch
            .column_by_name("bronze_contract_version")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(contract_ver_col.value(0), "2.0.0");

        let metrics_file = PathBuf::from(&m2.parquet_path)
            .parent()
            .unwrap()
            .join(format!("metrics-{}.json", m2.extraction_run_id));
        assert!(metrics_file.exists());
    }

    #[tokio::test]
    async fn run_ingest_resume_continues_from_checkpoint_and_skips_completed() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        store
            .save(&CheckpointState {
                retailer: "metro".to_string(),
                category: "done_cat".to_string(),
                next_offset: 20,
                pages_completed: 1,
                page_size: Some(20),
                extraction_run_id: Uuid::new_v4(),
                updated_at: Utc::now(),
                completed: true,
            })
            .unwrap();
        store
            .save(&CheckpointState {
                retailer: "metro".to_string(),
                category: "pending_cat".to_string(),
                next_offset: 20,
                pages_completed: 1,
                page_size: Some(20),
                extraction_run_id: Uuid::new_v4(),
                updated_at: Utc::now(),
                completed: false,
            })
            .unwrap();

        let page2_json = r#"[{"productId":"555","productName":"Aceite","items":[{"itemId":"5551","ean":"","name":"Aceite","sellers":[]}]}]"#;
        let (base_url, calls) = spawn_vtex_sequence(vec![(200, page2_json.to_string())]).await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::Metro, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "metro".to_string(),
            categories: vec!["done_cat".to_string(), "pending_cat".to_string()],
            page_size: 20,
            max_pages: 2,
            output_dir: dir.path().to_path_buf(),
            resume: true,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        let metrics = run_ingest(&adapter, &http, &cfg).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.pages_fetched, 1);
        assert_eq!(metrics.products_recovered, 1);
        assert_eq!(metrics.requested_postal_code, None);
        assert_eq!(
            metrics.business_date,
            business_date_lima(metrics.started_at).to_string()
        );

        let updated_cp = store.load("metro", "pending_cat").unwrap().unwrap();
        assert!(updated_cp.completed);
        assert_eq!(updated_cp.pages_completed, 2);
        assert_eq!(updated_cp.next_offset, 21);
    }

    #[tokio::test]
    async fn run_ingest_returns_err_on_partial_category_failure_while_persisting_succeeded_work() {
        let dir = TempDir::new().unwrap();
        let ok_json = r#"[{"productId":"10","productName":"Leche","items":[{"itemId":"101","ean":"7750000000001","name":"Leche","sellers":[]}]}]"#;
        let (base_url, _) = spawn_vtex_sequence(vec![
            (200, ok_json.to_string()),
            (404, "not found".to_string()),
        ])
        .await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string(), "broken_cat".to_string()],
            page_size: 20,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        let err = run_ingest(&adapter, &http, &cfg).await.unwrap_err();
        assert!(format!("{err:#}").contains("404"));

        let store = CheckpointStore::new(dir.path());
        assert!(store.load("plazavea", "lacteos").unwrap().is_some());
        assert!(store.load("plazavea", "broken_cat").unwrap().is_none());

        let dedup = PayloadDeduplicator::open(dir.path()).unwrap();
        assert_eq!(dedup.len(), 1);
    }

    #[tokio::test]
    async fn run_ingest_does_not_persist_dedup_or_checkpoints_if_parquet_write_fails() {
        let dir = TempDir::new().unwrap();
        let run_date = business_date_lima(Utc::now());
        let blocking_file = dir
            .path()
            .join(format!("dt={}", run_date.format("%Y-%m-%d")));
        std::fs::write(&blocking_file, b"not a directory").unwrap();

        let ok_json = r#"[{"productId":"10","productName":"Leche","items":[{"itemId":"101","ean":"7750000000001","name":"Leche","sellers":[]}]}]"#;
        let (base_url, _) = spawn_vtex_sequence(vec![(200, ok_json.to_string())]).await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 20,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        assert!(run_ingest(&adapter, &http, &cfg).await.is_err());

        let seen_path = dir.path().join("blobs").join("seen_hashes.json");
        assert!(
            !seen_path.exists(),
            "seen_hashes.json must not be persisted when Parquet write fails"
        );
        let cp_store = CheckpointStore::new(dir.path());
        assert!(
            cp_store.load("plazavea", "lacteos").unwrap().is_none(),
            "checkpoint must not be persisted when Parquet write fails"
        );
    }

    #[tokio::test]
    async fn run_ingest_resume_uses_saved_checkpoint_page_size_when_cli_differs() {
        let dir = TempDir::new().unwrap();
        let store = CheckpointStore::new(dir.path());
        store
            .save(&CheckpointState {
                retailer: "metro".to_string(),
                category: "lacteos".to_string(),
                next_offset: 10,
                pages_completed: 1,
                page_size: Some(10),
                extraction_run_id: Uuid::new_v4(),
                updated_at: Utc::now(),
                completed: false,
            })
            .unwrap();
        store
            .save(&CheckpointState {
                retailer: "metro".to_string(),
                category: "abarrotes".to_string(),
                next_offset: 5,
                pages_completed: 1,
                page_size: None,
                extraction_run_id: Uuid::new_v4(),
                updated_at: Utc::now(),
                completed: false,
            })
            .unwrap();

        let page_json = r#"[{"productId":"700","productName":"Yogurt","items":[{"itemId":"7001","ean":"","name":"Yogurt","sellers":[]}]}]"#;
        let (base_url, _, requests) = spawn_vtex_sequence_with_requests(vec![
            (200, page_json.to_string()),
            (200, page_json.to_string()),
        ])
        .await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::Metro, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "metro".to_string(),
            categories: vec!["lacteos".to_string(), "abarrotes".to_string()],
            page_size: 25,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: true,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        run_ingest(&adapter, &http, &cfg).await.unwrap();

        let recorded = requests.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2);
        assert!(
            recorded[0].contains("_from=10&_to=19"),
            "expected saved checkpoint page_size=10 (_from=10&_to=19), got: {}",
            recorded[0]
        );
        assert!(
            recorded[1].contains("_from=5&_to=29"),
            "expected fallback to CLI page_size=25 when checkpoint page_size is None (_from=5&_to=29), got: {}",
            recorded[1]
        );

        let lacteos_cp = store.load("metro", "lacteos").unwrap().unwrap();
        assert_eq!(lacteos_cp.page_size, Some(10));
        let abarrotes_cp = store.load("metro", "abarrotes").unwrap().unwrap();
        assert_eq!(abarrotes_cp.page_size, Some(25));
    }

    fn read_parquet_postal_codes(path: &std::path::Path) -> Vec<Option<String>> {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let col = batch
                .column_by_name("postal_code")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                values.push((!col.is_null(row)).then(|| col.value(row).to_string()));
            }
        }
        values
    }

    #[derive(Clone, Default)]
    struct SharedLogBuffer(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for SharedLogBuffer {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for SharedLogBuffer {
        type Writer = SharedLogBuffer;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    fn install_warn_log_capture() -> (SharedLogBuffer, tracing::subscriber::DefaultGuard) {
        let log_buf = SharedLogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .with_writer(log_buf.clone())
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (log_buf, guard)
    }

    struct GeoAwareVtexAdapter(VtexAdapter);

    #[async_trait::async_trait]
    impl RetailerAdapter for GeoAwareVtexAdapter {
        fn retailer_slug(&self) -> &'static str {
            self.0.retailer_slug()
        }

        fn extractor_version(&self) -> &'static str {
            self.0.extractor_version()
        }

        fn build_page_url(
            &self,
            category_or_query: &str,
            offset: usize,
            page_size: usize,
        ) -> String {
            self.0.build_page_url(category_or_query, offset, page_size)
        }

        fn parse_page_payload(
            &self,
            body: &str,
            ctx: &FetchContext,
        ) -> anyhow::Result<Vec<crate::domain::Product>> {
            self.0.parse_page_payload(body, ctx)
        }

        async fn fetch_page(
            &self,
            category_or_query: &str,
            offset: usize,
            page_size: usize,
            ctx: &FetchContext,
        ) -> anyhow::Result<crate::adapter::PageBatch> {
            self.0
                .fetch_page(category_or_query, offset, page_size, ctx)
                .await
        }

        fn volatile_payload_keys(&self) -> &'static [&'static str] {
            self.0.volatile_payload_keys()
        }

        fn applies_geo_context(&self) -> bool {
            true
        }
    }

    #[test]
    fn business_date_lima_uses_utc_minus_5() {
        let d = |h, m| business_date_lima(Utc.with_ymd_and_hms(2026, 11, 14, h, m, 0).unwrap());
        assert_eq!(d(3, 10), NaiveDate::from_ymd_opt(2026, 11, 13).unwrap());
        assert_eq!(d(4, 59), NaiveDate::from_ymd_opt(2026, 11, 13).unwrap());
        assert_eq!(d(5, 0), NaiveDate::from_ymd_opt(2026, 11, 14).unwrap());
        assert_eq!(d(23, 59), NaiveDate::from_ymd_opt(2026, 11, 14).unwrap());
        assert_eq!(
            business_date_lima(Utc.with_ymd_and_hms(2026, 1, 1, 4, 59, 59).unwrap()),
            NaiveDate::from_ymd_opt(2025, 12, 31).unwrap()
        );
    }

    #[tokio::test]
    async fn run_ingest_does_not_stamp_unapplied_postal_code() {
        let dir = TempDir::new().unwrap();
        let prod_json = r#"[{"productId":"20402646","productName":"Leche Gloria","brand":"Gloria","items":[{"itemId":"1001","ean":"7750243012345","name":"Leche Gloria 390g","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":4.5,"Price":4.2,"AvailableQuantity":20}}]},{"itemId":"1002","ean":"7750243012346","name":"Leche Gloria Pack","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":25.0,"Price":23.5,"AvailableQuantity":5}}]}]}]"#;
        let (base_url, _) = spawn_vtex_sequence(vec![(200, prod_json.to_string())]).await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http.clone());
        assert!(!adapter.applies_geo_context());

        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 20,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: Some("150101".to_string()),
        };

        let (log_buf, _guard) = install_warn_log_capture();

        let metrics = run_ingest(&adapter, &http, &cfg).await.unwrap();
        assert_eq!(metrics.requested_postal_code.as_deref(), Some("150101"));
        assert_eq!(
            metrics.business_date,
            business_date_lima(metrics.started_at).to_string()
        );
        assert!(
            metrics
                .parquet_path
                .contains(&format!("dt={}", metrics.business_date)),
            "expected parquet_path {} to use dt={}",
            metrics.parquet_path,
            metrics.business_date
        );

        let postal_codes = read_parquet_postal_codes(std::path::Path::new(&metrics.parquet_path));
        assert_eq!(postal_codes, vec![None, None]);

        let metrics_path = PathBuf::from(&metrics.parquet_path)
            .parent()
            .unwrap()
            .join(format!("metrics-{}.json", metrics.extraction_run_id));
        let reloaded: RunMetrics =
            serde_json::from_str(&std::fs::read_to_string(&metrics_path).unwrap()).unwrap();
        assert_eq!(reloaded.requested_postal_code.as_deref(), Some("150101"));
        assert_eq!(reloaded.business_date, metrics.business_date);

        let logs = String::from_utf8_lossy(&log_buf.0.lock().unwrap()).to_string();
        assert!(
            logs.contains("150101") && logs.contains("not applied"),
            "expected warning about unapplied postal_code, got logs: {logs}"
        );
    }

    #[tokio::test]
    async fn run_ingest_stamps_postal_code_when_adapter_applies_geo_context() {
        let dir = TempDir::new().unwrap();
        let prod_json = r#"[{"productId":"20402646","productName":"Leche Gloria","brand":"Gloria","items":[{"itemId":"1001","ean":"7750243012345","name":"Leche Gloria 390g","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":4.5,"Price":4.2,"AvailableQuantity":20}}]}]}]"#;
        let (base_url, _) = spawn_vtex_sequence(vec![(200, prod_json.to_string())]).await;

        let http = fast_http();
        let adapter = GeoAwareVtexAdapter(VtexAdapter::with_base_url(
            VtexRetailer::PlazaVea,
            &base_url,
            http.clone(),
        ));
        assert!(adapter.applies_geo_context());

        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 20,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: Some("150101".to_string()),
        };

        let (log_buf, _guard) = install_warn_log_capture();

        let metrics = run_ingest(&adapter, &http, &cfg).await.unwrap();
        assert_eq!(metrics.requested_postal_code.as_deref(), Some("150101"));

        let postal_codes = read_parquet_postal_codes(std::path::Path::new(&metrics.parquet_path));
        assert_eq!(postal_codes, vec![Some("150101".to_string())]);

        let logs = String::from_utf8_lossy(&log_buf.0.lock().unwrap()).to_string();
        assert!(
            logs.is_empty(),
            "expected no unapplied postal_code warning when adapter applies geo context, got: {logs}"
        );
    }

    #[tokio::test]
    async fn run_ingest_marks_truncated_when_max_pages_stops_with_more_pages() {
        let dir = TempDir::new().unwrap();
        let page1 = r#"[
            {"productId":"101","productName":"Prod 1","items":[{"itemId":"1011","ean":"7750000000011","name":"Prod 1","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":5.0,"Price":4.5,"AvailableQuantity":10}}]}]},
            {"productId":"102","productName":"Prod 2","items":[{"itemId":"1021","ean":"7750000000028","name":"Prod 2","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":6.0,"Price":5.5,"AvailableQuantity":8}}]}]}
        ]"#;
        let page2 = r#"[
            {"productId":"103","productName":"Prod 3","items":[{"itemId":"1031","ean":"7750000000035","name":"Prod 3","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":7.0,"Price":6.5,"AvailableQuantity":4}}]}]},
            {"productId":"104","productName":"Prod 4","items":[{"itemId":"1041","ean":"7750000000042","name":"Prod 4","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":8.0,"Price":7.5,"AvailableQuantity":2}}]}]}
        ]"#;
        let (base_url, calls) =
            spawn_vtex_sequence(vec![(206, page1.to_string()), (206, page2.to_string())]).await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 2,
            max_pages: 1,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        let metrics = run_ingest(&adapter, &http, &cfg).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(metrics.status, "truncated");
        assert_eq!(metrics.bronze_contract_version, "2.0.0");
        assert_eq!(metrics.categories_detail.len(), 1);
        assert_eq!(metrics.categories_detail[0].category, "lacteos");
        assert_eq!(metrics.categories_detail[0].stop_reason, "max_pages");
        assert_eq!(metrics.categories_detail[0].pages_fetched, 1);
        assert_eq!(metrics.categories_detail[0].products, 2);
        assert_eq!(metrics.categories_detail[0].skus, 2);
        assert_eq!(metrics.categories_detail[0].next_offset, 2);

        let cp = CheckpointStore::new(dir.path())
            .load("plazavea", "lacteos")
            .unwrap()
            .unwrap();
        assert!(!cp.completed);
        assert_eq!(cp.next_offset, 2);
    }

    #[tokio::test]
    async fn run_ingest_reports_exhausted_as_success_and_counts_products_without_skus() {
        let dir = TempDir::new().unwrap();
        let page1 = r#"[
            {"productId":"201","productName":"With SKU","items":[{"itemId":"2011","ean":"7750000000219","name":"With SKU","sellers":[{"sellerId":"1","commertialOffer":{"ListPrice":5.0,"Price":4.5,"AvailableQuantity":10}}]}]},
            {"productId":"202","productName":"Without SKU","items":[]}
        ]"#;
        let (base_url, calls) =
            spawn_vtex_sequence(vec![(206, page1.to_string()), (200, "[]".to_string())]).await;

        let http = fast_http();
        let adapter = VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url, http.clone());
        let cfg = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 2,
            max_pages: 5,
            output_dir: dir.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };

        let metrics = run_ingest(&adapter, &http, &cfg).await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(metrics.status, "success");
        assert_eq!(metrics.bronze_contract_version, "2.0.0");
        assert_eq!(metrics.categories_detail.len(), 1);
        assert_eq!(metrics.categories_detail[0].stop_reason, "exhausted");
        assert_eq!(metrics.categories_detail[0].pages_fetched, 2);
        assert_eq!(metrics.categories_detail[0].products, 2);
        assert_eq!(metrics.categories_detail[0].skus, 1);
        assert_eq!(metrics.categories_detail[0].products_without_skus, 1);
        assert_eq!(metrics.products_recovered, 2);
        assert_eq!(metrics.skus_recovered, 1);

        let cp = CheckpointStore::new(dir.path())
            .load("plazavea", "lacteos")
            .unwrap()
            .unwrap();
        assert!(cp.completed);
    }

    #[tokio::test]
    async fn run_ingest_zero_page_error_leaves_no_parquet_and_partial_error_overrides_truncated() {
        // Case 1: first page fails -> pages_fetched == 0 -> writer aborted cleanly, no parquet/tmp/metrics
        let dir1 = TempDir::new().unwrap();
        let (base_url1, _) = spawn_vtex_sequence(vec![(404, "not found".to_string())]).await;
        let http1 = fast_http();
        let adapter1 =
            VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url1, http1.clone());
        let cfg1 = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string()],
            page_size: 2,
            max_pages: 1,
            output_dir: dir1.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };
        assert!(run_ingest(&adapter1, &http1, &cfg1).await.is_err());

        let run_date = business_date_lima(Utc::now());
        let part_dir1 = dir1
            .path()
            .join(format!("dt={}", run_date.format("%Y-%m-%d")))
            .join("retailer=plazavea");
        if part_dir1.exists() {
            assert!(
                std::fs::read_dir(&part_dir1).unwrap().next().is_none(),
                "zero-page failure must not leave any parquet or temp files"
            );
        }

        // Case 2: cat1 stops at max_pages, cat2 fails -> metrics status is "partial_error"
        let dir2 = TempDir::new().unwrap();
        let full_page = r#"[
            {"productId":"301","productName":"P1","items":[{"itemId":"3011","ean":"7750000000318","name":"P1","sellers":[]}]},
            {"productId":"302","productName":"P2","items":[{"itemId":"3021","ean":"7750000000325","name":"P2","sellers":[]}]}
        ]"#;
        let (base_url2, _) = spawn_vtex_sequence(vec![
            (206, full_page.to_string()),
            (404, "missing".to_string()),
        ])
        .await;
        let http2 = fast_http();
        let adapter2 =
            VtexAdapter::with_base_url(VtexRetailer::PlazaVea, &base_url2, http2.clone());
        let cfg2 = IngestConfig {
            retailer: "plazavea".to_string(),
            categories: vec!["lacteos".to_string(), "broken_cat".to_string()],
            page_size: 2,
            max_pages: 1,
            output_dir: dir2.path().to_path_buf(),
            resume: false,
            market_id: "PE".to_string(),
            postal_code: None,
        };
        assert!(run_ingest(&adapter2, &http2, &cfg2).await.is_err());

        let part_dir2 = dir2
            .path()
            .join(format!(
                "dt={}",
                business_date_lima(Utc::now()).format("%Y-%m-%d")
            ))
            .join("retailer=plazavea");
        let metrics_file = std::fs::read_dir(&part_dir2)
            .unwrap()
            .map(|e| e.unwrap().path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .is_some_and(|n| n.starts_with("metrics-") && n.ends_with(".json"))
            })
            .expect("metrics json must be persisted on partial_error");
        let metrics: RunMetrics =
            serde_json::from_str(&std::fs::read_to_string(&metrics_file).unwrap()).unwrap();
        assert_eq!(metrics.status, "partial_error");
        assert_eq!(metrics.categories_detail.len(), 2);
        assert_eq!(metrics.categories_detail[0].stop_reason, "max_pages");
        assert_eq!(metrics.categories_detail[1].stop_reason, "error");
        assert_eq!(metrics.categories_detail[1].pages_fetched, 0);
    }
}
