use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunMetrics {
    pub extraction_run_id: Uuid,
    pub retailer: String,
    pub extractor_version: String,
    pub categories: Vec<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub status: String,
    pub error: Option<String>,
    pub products_recovered: usize,
    pub skus_recovered: usize,
    pub pages_fetched: usize,
    pub duplicates_detected: usize,
    pub requests_ok: u64,
    pub requests_error: u64,
    pub avg_latency_ms: f64,
    pub mb_transferred: f64,
    pub total_duration_secs: f64,
    pub peak_ram_mb: f64,
    pub parquet_path: String,
}

fn parse_kb_line(line: &str) -> Option<f64> {
    let kb_str = line.split(':').nth(1)?.split_whitespace().next()?;
    let kb: f64 = kb_str.parse().ok()?;
    Some(kb / 1024.0)
}

/// Parses Linux `/proc/self/status` contents for peak resident memory (`VmHWM`, fallback `VmRSS`) in MB.
pub fn parse_proc_status_ram_mb(contents: &str) -> Option<f64> {
    let mut vm_rss: Option<f64> = None;
    for line in contents.lines() {
        if line.starts_with("VmHWM:") {
            if let Some(mb) = parse_kb_line(line) {
                return Some(mb);
            }
        } else if line.starts_with("VmRSS:") {
            vm_rss = parse_kb_line(line);
        }
    }
    vm_rss
}

pub fn read_peak_ram_mb() -> f64 {
    fs::read_to_string("/proc/self/status")
        .ok()
        .as_deref()
        .and_then(parse_proc_status_ram_mb)
        .unwrap_or(0.0)
}

impl RunMetrics {
    pub fn compute_avg_latency_ms(latencies_ms: &[f64]) -> f64 {
        if latencies_ms.is_empty() {
            return 0.0;
        }
        let sum: f64 = latencies_ms.iter().sum();
        sum / latencies_ms.len() as f64
    }

    pub fn bytes_to_mb(bytes: usize) -> f64 {
        bytes as f64 / (1024.0 * 1024.0)
    }

    pub fn write_json(&self, dir: &Path) -> anyhow::Result<PathBuf> {
        let out_path = dir.join(format!("metrics-{}.json", self.extraction_run_id));
        let payload = serde_json::to_vec_pretty(self)?;
        crate::atomic_write(&out_path, &payload)?;
        Ok(out_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn parse_proc_status_ram_mb_prefers_vmhwm_and_falls_back_to_vmrss() {
        let status_both =
            "Name:\tingestor\nVmPeak:\t  102400 kB\nVmHWM:\t   45056 kB\nVmRSS:\t   40960 kB\n";
        assert_eq!(parse_proc_status_ram_mb(status_both), Some(44.0));

        let status_rss_only = "Name:\tingestor\nVmRSS:\t   20480 kB\n";
        assert_eq!(parse_proc_status_ram_mb(status_rss_only), Some(20.0));

        assert_eq!(parse_proc_status_ram_mb("Name:\tingestor\n"), None);
    }

    #[test]
    fn run_metrics_computes_averages_and_persists_json() {
        let dir = TempDir::new().unwrap();
        let run_id = Uuid::new_v4();

        assert_eq!(RunMetrics::compute_avg_latency_ms(&[]), 0.0);
        assert!((RunMetrics::compute_avg_latency_ms(&[100.0, 200.0]) - 150.0).abs() < 1e-9);
        assert!((RunMetrics::bytes_to_mb(1_048_576) - 1.0).abs() < 1e-9);

        let now = Utc::now();
        let metrics = RunMetrics {
            extraction_run_id: run_id,
            retailer: "plazavea".to_string(),
            extractor_version: "vtex-catalog-v1.0.0".to_string(),
            categories: vec!["lacteos".to_string(), "abarrotes".to_string()],
            started_at: now,
            finished_at: now,
            status: "success".to_string(),
            error: None,
            products_recovered: 40,
            skus_recovered: 45,
            pages_fetched: 2,
            duplicates_detected: 5,
            requests_ok: 2,
            requests_error: 0,
            avg_latency_ms: 150.0,
            mb_transferred: 1.0,
            total_duration_secs: 1.25,
            peak_ram_mb: read_peak_ram_mb(),
            parquet_path: "data/bronze/dt=2026-11-13/retailer=plazavea/part.parquet".to_string(),
        };

        let out_path = metrics.write_json(dir.path()).unwrap();
        assert_eq!(out_path, dir.path().join(format!("metrics-{run_id}.json")));
        assert!(out_path.exists());

        let raw = fs::read_to_string(&out_path).unwrap();
        let reloaded: RunMetrics = serde_json::from_str(&raw).unwrap();
        assert_eq!(reloaded, metrics);
    }
}
