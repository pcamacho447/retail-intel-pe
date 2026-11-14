use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use governor::{Quota, RateLimiter};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, ACCEPT_LANGUAGE};

#[derive(Debug, Clone)]
pub struct HttpResponseData {
    pub status: u16,
    pub body: String,
    pub bytes: usize,
    pub latency_ms: f64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct HttpStats {
    pub requests_ok: u64,
    pub requests_error: u64,
}

pub struct RateLimitedHttpClient {
    client: reqwest::Client,
    limiter: RateLimiter<NotKeyed, InMemoryState, DefaultClock>,
    max_retries: u32,
    base_backoff_ms: u64,
    requests_ok: AtomicU64,
    requests_error: AtomicU64,
}

impl RateLimitedHttpClient {
    /// Creates an HTTP client enforcing <= 2 req/s, max 3 retries with 500ms*2^attempt backoff,
    /// 20s timeout, and a realistic browser User-Agent.
    pub fn new() -> anyhow::Result<Self> {
        Self::with_config(
            NonZeroU32::new(2).expect("2 is non-zero"),
            3,
            500,
            Duration::from_secs(20),
        )
    }

    pub fn with_config(
        requests_per_second: NonZeroU32,
        max_retries: u32,
        base_backoff_ms: u64,
        timeout: Duration,
    ) -> anyhow::Result<Self> {
        let mut headers = HeaderMap::new();
        headers.insert(
            ACCEPT,
            HeaderValue::from_static(
                "application/json, text/html, application/xhtml+xml;q=0.9, */*;q=0.8",
            ),
        );
        headers.insert(
            ACCEPT_LANGUAGE,
            HeaderValue::from_static("es-PE,es;q=0.9,en;q=0.8"),
        );

        let client = reqwest::Client::builder()
            .timeout(timeout)
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
            .default_headers(headers)
            .build()
            .context("failed to build reqwest HTTP client")?;

        let quota = Quota::per_second(requests_per_second)
            .allow_burst(NonZeroU32::new(1).expect("1 is non-zero"));
        let limiter = RateLimiter::direct(quota);

        Ok(Self {
            client,
            limiter,
            max_retries,
            base_backoff_ms,
            requests_ok: AtomicU64::new(0),
            requests_error: AtomicU64::new(0),
        })
    }

    pub async fn get_text(&self, url: &str) -> anyhow::Result<HttpResponseData> {
        for attempt in 0..=self.max_retries {
            self.limiter.until_ready().await;
            let req_start = Instant::now();

            match self.client.get(url).send().await {
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    if resp.status().is_success() {
                        match resp.text().await {
                            Ok(body) => {
                                let bytes = body.len();
                                let latency_ms = req_start.elapsed().as_secs_f64() * 1000.0;
                                self.requests_ok.fetch_add(1, Ordering::Relaxed);
                                return Ok(HttpResponseData {
                                    status,
                                    body,
                                    bytes,
                                    latency_ms,
                                });
                            }
                            Err(err) => {
                                self.requests_error.fetch_add(1, Ordering::Relaxed);
                                if attempt < self.max_retries {
                                    self.sleep_backoff(
                                        attempt,
                                        url,
                                        &format!("body read error: {err}"),
                                    )
                                    .await;
                                    continue;
                                }
                                return Err(anyhow!(
                                    "HTTP response body error from {url} (status {status}) after {} retries: {err:#}",
                                    self.max_retries
                                ));
                            }
                        }
                    } else if status == 429 || (500..=599).contains(&status) {
                        self.requests_error.fetch_add(1, Ordering::Relaxed);
                        let body_snippet: String = resp
                            .text()
                            .await
                            .unwrap_or_default()
                            .chars()
                            .take(200)
                            .collect();
                        if attempt < self.max_retries {
                            self.sleep_backoff(attempt, url, &format!("HTTP {status}"))
                                .await;
                            continue;
                        }
                        return Err(anyhow!(
                            "HTTP request to {url} failed with status {status} after {} retries: {body_snippet:?}",
                            self.max_retries
                        ));
                    } else {
                        self.requests_error.fetch_add(1, Ordering::Relaxed);
                        let body_snippet: String = resp
                            .text()
                            .await
                            .unwrap_or_default()
                            .chars()
                            .take(200)
                            .collect();
                        return Err(anyhow!(
                            "HTTP request to {url} failed with non-retryable status {status}: {body_snippet:?}"
                        ));
                    }
                }
                Err(err) => {
                    self.requests_error.fetch_add(1, Ordering::Relaxed);
                    if attempt < self.max_retries {
                        self.sleep_backoff(attempt, url, &format!("transport error: {err}"))
                            .await;
                        continue;
                    }
                    return Err(anyhow!(
                        "HTTP request to {url} failed after {} retries: {err:#}",
                        self.max_retries
                    ));
                }
            }
        }

        unreachable!("retry loop returns on final attempt")
    }

    async fn sleep_backoff(&self, attempt: u32, url: &str, reason: &str) {
        let shift = attempt.min(16);
        let backoff_ms = self.base_backoff_ms.saturating_mul(1u64 << shift);
        tracing::warn!(
            url = url,
            attempt = attempt + 1,
            max_retries = self.max_retries,
            backoff_ms = backoff_ms,
            reason = reason,
            "Retrying HTTP request with exponential backoff"
        );
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }

    pub fn stats(&self) -> HttpStats {
        HttpStats {
            requests_ok: self.requests_ok.load(Ordering::Relaxed),
            requests_error: self.requests_error.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_mock_server(responses: Vec<(u16, &'static str)>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let responses = Arc::new(responses);
        let idx = Arc::new(AtomicU64::new(0));

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let responses = responses.clone();
                let idx = idx.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let _ = socket.read(&mut buf).await;
                    let i = idx.fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize;
                    let (status, body) = responses
                        .get(i)
                        .copied()
                        .unwrap_or_else(|| *responses.last().unwrap());
                    let reason = match status {
                        200 => "OK",
                        206 => "Partial Content",
                        404 => "Not Found",
                        429 => "Too Many Requests",
                        500 => "Internal Server Error",
                        503 => "Service Unavailable",
                        _ => "Status",
                    };
                    let raw = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(raw.as_bytes()).await;
                });
            }
        });

        format!("http://{addr}")
    }

    #[tokio::test]
    async fn http_client_enforces_rate_limit_at_most_2_req_per_sec() {
        let base_url = spawn_mock_server(vec![(200, "ok")]).await;
        let client = RateLimitedHttpClient::new().unwrap();

        let start = Instant::now();
        for _ in 0..3 {
            let resp = client.get_text(&format!("{base_url}/test")).await.unwrap();
            assert_eq!(resp.status, 200);
        }
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(900),
            "expected >= 900ms for 3 requests at <=2 req/s, got {:?}",
            elapsed
        );
    }

    #[tokio::test]
    async fn http_client_retries_429_and_503_with_backoff_and_succeeds() {
        let base_url = spawn_mock_server(vec![
            (429, "rate limited"),
            (503, "busy"),
            (200, "recovered"),
        ])
        .await;
        let client = RateLimitedHttpClient::with_config(
            NonZeroU32::new(20).unwrap(),
            3,
            10,
            Duration::from_secs(5),
        )
        .unwrap();

        let resp = client.get_text(&format!("{base_url}/retry")).await.unwrap();
        assert_eq!(resp.status, 200);
        assert_eq!(resp.body, "recovered");
        assert_eq!(resp.bytes, 9);
        assert_eq!(
            client.stats(),
            HttpStats {
                requests_ok: 1,
                requests_error: 2,
            }
        );
    }

    #[tokio::test]
    async fn http_client_fails_after_exhausting_retries_on_persistent_500() {
        let base_url = spawn_mock_server(vec![(500, "boom")]).await;
        let client = RateLimitedHttpClient::with_config(
            NonZeroU32::new(20).unwrap(),
            3,
            5,
            Duration::from_secs(5),
        )
        .unwrap();

        let err = client
            .get_text(&format!("{base_url}/fail"))
            .await
            .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("500"), "expected status 500 in error: {msg}");
        assert_eq!(
            client.stats(),
            HttpStats {
                requests_ok: 0,
                requests_error: 4,
            }
        );
    }

    #[tokio::test]
    async fn http_client_does_not_retry_non_429_4xx_errors() {
        let base_url = spawn_mock_server(vec![(404, "not found"), (200, "should not reach")]).await;
        let client = RateLimitedHttpClient::with_config(
            NonZeroU32::new(20).unwrap(),
            3,
            5,
            Duration::from_secs(5),
        )
        .unwrap();

        let err = client
            .get_text(&format!("{base_url}/missing"))
            .await
            .unwrap_err();
        assert!(format!("{err:#}").contains("404"));
        assert_eq!(
            client.stats(),
            HttpStats {
                requests_ok: 0,
                requests_error: 1,
            }
        );
    }

    #[tokio::test]
    async fn http_client_accepts_206_partial_content() {
        let base_url = spawn_mock_server(vec![(206, "[]")]).await;
        let client = RateLimitedHttpClient::with_config(
            NonZeroU32::new(20).unwrap(),
            3,
            5,
            Duration::from_secs(5),
        )
        .unwrap();

        let resp = client.get_text(&format!("{base_url}/vtex")).await.unwrap();
        assert_eq!(resp.status, 206);
        assert_eq!(resp.body, "[]");
    }
}
