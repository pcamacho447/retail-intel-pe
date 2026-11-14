use std::path::PathBuf;
use std::time::Instant;

use anyhow::Context;
use chrono::Utc;
use uuid::Uuid;

use crate::adapter::{FetchContext, RetailerAdapter};
use crate::checkpoint::{CheckpointState, CheckpointStore};
use crate::dedup::PayloadDeduplicator;
use crate::domain::BronzeSkuRow;
use crate::http::RateLimitedHttpClient;
use crate::metrics::{read_peak_ram_mb, RunMetrics};
use crate::storage::parquet_writer::write_bronze_parquet;

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
    let run_date = Utc::now().date_naive();
    let slug = adapter.retailer_slug();

    let mut dedup = PayloadDeduplicator::open(&config.output_dir)?;
    let checkpoints = CheckpointStore::new(&config.output_dir);

    let base_ctx = FetchContext::for_run(
        extraction_run_id,
        config.market_id.clone(),
        config.postal_code.clone(),
    );

    let mut all_rows: Vec<BronzeSkuRow> = Vec::new();
    let mut pending_checkpoints: Vec<CheckpointState> = Vec::new();
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

                    for product in &batch.products {
                        products_recovered += 1;
                        let is_dup = dedup.check_and_mark(&product.payload_hash);
                        if is_dup {
                            duplicates_detected += 1;
                        }
                        let bronze_rows = product.to_bronze_rows(is_dup);
                        skus_recovered += bronze_rows.len();
                        all_rows.extend(bronze_rows);
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
                    break;
                }
            }
        }

        if let Some(cp) = latest_cp {
            pending_checkpoints.push(cp);
        }
    }

    if pages_fetched == 0 {
        if let Some(err) = last_error {
            return Err(err);
        }
    }

    // Write Parquet first so dedup hashes and checkpoints are only committed once Bronze data is durable.
    let parquet_path = write_bronze_parquet(
        &config.output_dir,
        run_date,
        slug,
        extraction_run_id,
        &all_rows,
    )?;
    dedup.persist()?;
    for cp in &pending_checkpoints {
        checkpoints.save(cp)?;
    }

    let http_stats = http.stats();
    let error_msg = last_error.as_ref().map(|err| format!("{err:#}"));
    let status = if error_msg.is_some() {
        "partial_error"
    } else {
        "success"
    };

    let metrics = RunMetrics {
        extraction_run_id,
        retailer: slug.to_string(),
        extractor_version: adapter.extractor_version().to_string(),
        categories: config.categories.clone(),
        started_at: started_utc,
        finished_at: Utc::now(),
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
    use chrono::Utc;
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
        let run_date = Utc::now().date_naive();
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
}
