use std::fmt::Write;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde_json::Value;

use crate::adapter::{FetchContext, PageBatch, RetailerAdapter};
use crate::adapters::{absolutize_url, encode_url_segment, value_as_string};
use crate::dedup::compute_payload_hash;
use crate::domain::{normalize_opt_string, Product, SkuItem};
use crate::http::RateLimitedHttpClient;

const TOTTUS_BASE_URL: &str = "https://tottus.falabella.com.pe";

pub struct FalabellaListingAdapter {
    base_url: String,
    http: Arc<RateLimitedHttpClient>,
}

impl FalabellaListingAdapter {
    pub fn new_tottus(http: Arc<RateLimitedHttpClient>) -> Self {
        Self::with_base_url(TOTTUS_BASE_URL, http)
    }

    pub fn with_base_url(base_url: impl Into<String>, http: Arc<RateLimitedHttpClient>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        }
    }
}

fn extract_next_data_json(body: &str, http_status: u16) -> anyhow::Result<Value> {
    let trimmed = body.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return serde_json::from_str(trimmed).with_context(|| {
            let snippet: String = trimmed.chars().take(200).collect();
            format!(
                "Falabella/Tottus JSON payload invalid (http_status={http_status}): snippet={snippet:?}"
            )
        });
    }

    let marker = if let Some(pos) = body.find(r#"id="__NEXT_DATA__""#) {
        pos
    } else if let Some(pos) = body.find(r"id='__NEXT_DATA__'") {
        pos
    } else {
        let snippet: String = body.chars().take(200).collect();
        return Err(anyhow!(
            "Falabella/Tottus response missing __NEXT_DATA__ script and is not valid JSON (possible WAF/CAPTCHA block or schema change, http_status={http_status}): snippet={snippet:?}"
        ));
    };

    let after_marker = &body[marker..];
    let tag_end = after_marker.find('>').ok_or_else(|| {
        anyhow!("Falabella/Tottus malformed __NEXT_DATA__ script tag (http_status={http_status})")
    })?;
    let json_start = &after_marker[tag_end + 1..];
    let script_end = json_start.find("</script>").ok_or_else(|| {
        anyhow!("Falabella/Tottus unclosed __NEXT_DATA__ script tag (http_status={http_status})")
    })?;

    let raw_json = json_start[..script_end].trim();
    serde_json::from_str(raw_json).with_context(|| {
        let snippet: String = raw_json.chars().take(200).collect();
        format!(
            "Falabella/Tottus __NEXT_DATA__ script contains invalid JSON (http_status={http_status}): snippet={snippet:?}"
        )
    })
}

fn parse_price_value(raw: &Value) -> Option<f64> {
    let scalar = match raw {
        Value::Array(arr) => arr.first()?,
        other => other,
    };

    if let Some(num) = scalar.as_f64() {
        return if num.is_finite() && num > 0.0 {
            Some(num)
        } else {
            None
        };
    }

    let s = scalar.as_str()?;
    let segment = s.split('-').next().unwrap_or(s);
    let first_digit_pos = segment.find(|ch: char| ch.is_ascii_digit())?;
    let from_first_digit = &segment[first_digit_pos..];

    let filtered: String = from_first_digit
        .chars()
        .filter(|ch| ch.is_ascii_digit() || *ch == '.' || *ch == ',')
        .collect();

    if filtered.is_empty() {
        return None;
    }

    let normalized = if filtered.contains('.') && filtered.contains(',') {
        let last_dot = filtered.rfind('.').unwrap();
        let last_comma = filtered.rfind(',').unwrap();
        if last_dot > last_comma {
            filtered.replace(',', "")
        } else {
            filtered.replace('.', "").replace(',', ".")
        }
    } else if filtered.contains(',') {
        filtered.replace(',', ".")
    } else {
        filtered
    };

    let parsed: f64 = normalized.parse().ok()?;
    if parsed.is_finite() && parsed > 0.0 {
        Some(parsed)
    } else {
        None
    }
}

fn extract_falabella_prices(item: &Value) -> (Option<f64>, Option<f64>, Option<f64>) {
    let mut list_price: Option<f64> = None;
    let mut internet_price: Option<f64> = None;
    let mut event_price: Option<f64> = None;
    let mut card_price: Option<f64> = None;

    if let Some(prices) = item.get("prices").and_then(|v| v.as_array()) {
        for entry in prices {
            let kind = entry
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let Some(val) = entry.get("price").and_then(parse_price_value) else {
                continue;
            };
            match kind {
                "normalPrice" => list_price = Some(val),
                "internetPrice" => internet_price = Some(val),
                "eventPrice" => event_price = Some(val),
                "cmrPrice" => card_price = Some(val),
                _ => {}
            }
        }
    }

    let selling_price = match (event_price, internet_price) {
        (Some(e), Some(i)) => Some(e.min(i)),
        (a, b) => a.or(b),
    }
    .or(list_price);

    (list_price, selling_price, card_price)
}

fn parse_results_from_root(
    root: &Value,
    base_url: &str,
    extractor_version: &str,
    ctx: &FetchContext,
) -> anyhow::Result<Vec<Product>> {
    let results = root
        .pointer("/props/pageProps/results")
        .or_else(|| root.get("results"))
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            anyhow!(
                "Falabella/Tottus JSON missing props.pageProps.results array (possible schema change, http_status={})",
                ctx.http_status
            )
        })?;

    let mut products = Vec::with_capacity(results.len());
    for item in results {
        let Some(product_id) = value_as_string(item.get("productId")) else {
            tracing::warn!(
                http_status = ctx.http_status,
                "Falabella/Tottus result item missing productId; skipping item"
            );
            continue;
        };
        let raw_payload = serde_json::to_string(item)?;
        let payload_hash = compute_payload_hash(&raw_payload);
        let sku_id = value_as_string(item.get("skuId")).unwrap_or_else(|| product_id.clone());
        let raw_name = normalize_opt_string(
            item.get("displayName")
                .or_else(|| item.get("name"))
                .and_then(|v| v.as_str()),
        )
        .unwrap_or_else(|| product_id.clone());
        let brand = normalize_opt_string(item.get("brand").and_then(|v| v.as_str()));
        let category_path = normalize_opt_string(
            item.get("merchantCategoryId")
                .or_else(|| item.get("category"))
                .and_then(|v| v.as_str()),
        );
        let product_url = absolutize_url(base_url, item.get("url").and_then(|v| v.as_str()));
        let image_url = item
            .get("mediaUrls")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .and_then(|s| normalize_opt_string(Some(s)));
        let seller_id = normalize_opt_string(
            item.get("sellerId")
                .or_else(|| item.get("sellerName"))
                .and_then(|v| v.as_str()),
        );
        let (list_price, selling_price, card_price) = extract_falabella_prices(item);

        products.push(Product {
            retailer: "tottus".to_string(),
            market_id: ctx.market_id.clone(),
            store_id: ctx.store_id.clone(),
            product_id,
            raw_name: raw_name.clone(),
            brand,
            category_path,
            raw_description: None,
            product_url,
            currency: ctx.currency.clone(),
            sales_channel: ctx.sales_channel.clone(),
            postal_code: ctx.postal_code.clone(),
            scraped_at: ctx.scraped_at,
            extraction_run_id: ctx.extraction_run_id,
            extractor_version: extractor_version.to_string(),
            source_endpoint: ctx.source_endpoint.clone(),
            http_status: ctx.http_status,
            payload_hash,
            raw_payload,
            skus: vec![SkuItem {
                sku_id,
                ean: None,
                seller_id,
                raw_name,
                image_url,
                list_price,
                selling_price,
                card_price,
                available_qty: None,
            }],
        });
    }

    Ok(products)
}

fn has_more_pages(root: &Value, products_len: usize, page_size: usize) -> bool {
    if products_len == 0 {
        return false;
    }

    if let Some(pagination) = root.pointer("/props/pageProps/pagination") {
        let current_page = pagination.get("currentPage").and_then(Value::as_u64);
        let total_pages = pagination.get("totalPages").and_then(Value::as_u64);
        if let (Some(curr), Some(total)) = (current_page, total_pages) {
            return curr < total;
        }
        let count = pagination.get("count").and_then(Value::as_u64);
        let per_page = pagination.get("perPage").and_then(Value::as_u64);
        if let (Some(curr), Some(per), Some(total_count)) = (current_page, per_page, count) {
            return curr.saturating_mul(per) < total_count;
        }
    }

    page_size > 0 && products_len >= page_size
}

#[async_trait]
impl RetailerAdapter for FalabellaListingAdapter {
    fn retailer_slug(&self) -> &'static str {
        "tottus"
    }

    fn extractor_version(&self) -> &'static str {
        "falabella-listing-v1.0.0"
    }

    fn build_page_url(&self, category_or_query: &str, offset: usize, page_size: usize) -> String {
        let effective_size = page_size.max(1);
        let page = (offset / effective_size) + 1;
        let trimmed = category_or_query.trim();

        if trimmed.starts_with('/') {
            let sep = if trimmed.contains('?') { '&' } else { '?' };
            format!("{}{}{sep}page={page}", self.base_url, trimmed)
        } else {
            format!(
                "{}/tottus-pe/search?Ntt={}&page={page}",
                self.base_url,
                encode_url_segment(trimmed)
            )
        }
    }

    fn parse_page_payload(&self, body: &str, ctx: &FetchContext) -> anyhow::Result<Vec<Product>> {
        let root = extract_next_data_json(body, ctx.http_status)?;
        parse_results_from_root(&root, &self.base_url, self.extractor_version(), ctx)
    }

    async fn fetch_page(
        &self,
        category_or_query: &str,
        offset: usize,
        page_size: usize,
        ctx: &FetchContext,
    ) -> anyhow::Result<PageBatch> {
        let mut url = self.build_page_url(category_or_query, offset, page_size);
        if let Some(ref pc) = ctx.postal_code {
            if !url.contains("postalCode=") {
                let sep = if url.contains('?') { '&' } else { '?' };
                let _ = write!(url, "{sep}postalCode={}", encode_url_segment(pc));
            }
        }
        if let Some(ref sid) = ctx.store_id {
            if !url.contains("storeId=") {
                let sep = if url.contains('?') { '&' } else { '?' };
                let _ = write!(url, "{sep}storeId={}", encode_url_segment(sid));
            }
        }
        let resp = self.http.get_text(&url).await?;
        let page_ctx = ctx.clone().with_endpoint(url, resp.status);
        let root = extract_next_data_json(&resp.body, resp.status)?;
        let products =
            parse_results_from_root(&root, &self.base_url, self.extractor_version(), &page_ctx)?;
        let has_more = has_more_pages(&root, products.len(), page_size);

        Ok(PageBatch {
            products,
            has_more,
            next_offset: offset + page_size.max(1),
            raw_bytes: resp.bytes,
            http_status: resp.status,
            latency_ms: resp.latency_ms,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroU32;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use uuid::Uuid;

    fn test_http() -> Arc<RateLimitedHttpClient> {
        Arc::new(
            RateLimitedHttpClient::with_config(
                NonZeroU32::new(20).unwrap(),
                1,
                5,
                Duration::from_secs(5),
            )
            .unwrap(),
        )
    }

    #[test]
    fn falabella_build_page_url_computes_1_based_page_number() {
        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        assert_eq!(adapter.retailer_slug(), "tottus");
        assert_eq!(
            adapter.build_page_url("leche", 0, 20),
            "https://tottus.falabella.com.pe/tottus-pe/search?Ntt=leche&page=1"
        );
        assert_eq!(
            adapter.build_page_url("aceite vegetal", 20, 20),
            "https://tottus.falabella.com.pe/tottus-pe/search?Ntt=aceite%20vegetal&page=2"
        );
        assert_eq!(
            adapter.build_page_url("/tottus-pe/category/cat401", 40, 20),
            "https://tottus.falabella.com.pe/tottus-pe/category/cat401?page=3"
        );
    }

    #[test]
    fn falabella_parses_next_data_html_and_prices() {
        let html = r#"<!doctype html>
        <html><head><title>Tottus</title></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "pagination": {"count": 45, "perPage": 20, "currentPage": 1, "totalPages": 3},
              "results": [
                {
                  "productId": "129087939",
                  "skuId": "129087940",
                  "displayName": "Leche Gloria Evaporada Entera 390 gr",
                  "brand": "GLORIA",
                  "merchantCategoryId": "CATG100",
                  "url": "/tottus-pe/product/129087939/leche-gloria-evaporada-entera-390-gr/129087940",
                  "mediaUrls": ["https://media.falabella.com.pe/tottusPE/129087940_1/public"],
                  "sellerId": "TOTTUS",
                  "prices": [
                    {"type": "normalPrice", "price": ["4.80"]},
                    {"type": "internetPrice", "price": ["4.20"]},
                    {"type": "cmrPrice", "price": ["3.90"]}
                  ]
                },
                {
                  "productId": "5001",
                  "displayName": "Canasta Premium",
                  "brand": "TOTTUS",
                  "url": "https://tottus.falabella.com.pe/tottus-pe/product/5001/canasta",
                  "mediaUrls": [],
                  "prices": [
                    {"type": "normalPrice", "price": ["S/ 1,599.00"]},
                    {"type": "eventPrice", "price": ["S/ 1,299.50"]}
                  ]
                },
                {
                  "productId": "6001",
                  "skuId": "6002",
                  "displayName": "Arroz Costeno 750g",
                  "prices": [
                    {"type": "normalPrice", "price": ["4.50"]}
                  ]
                }
              ]
            }
          }
        }</script>
        </body></html>"#;

        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", Some("150101".to_string()))
            .with_endpoint(
                "https://tottus.falabella.com.pe/tottus-pe/search?Ntt=leche&page=1",
                200,
            );

        let products = adapter.parse_page_payload(html, &ctx).unwrap();
        assert_eq!(products.len(), 3);

        let p1 = &products[0];
        assert_eq!(p1.retailer, "tottus");
        assert_eq!(p1.product_id, "129087939");
        assert_eq!(p1.skus[0].sku_id, "129087940");
        assert_eq!(p1.brand.as_deref(), Some("GLORIA"));
        assert_eq!(p1.category_path.as_deref(), Some("CATG100"));
        assert_eq!(
            p1.product_url.as_deref(),
            Some("https://tottus.falabella.com.pe/tottus-pe/product/129087939/leche-gloria-evaporada-entera-390-gr/129087940")
        );
        assert_eq!(p1.skus[0].seller_id.as_deref(), Some("TOTTUS"));
        assert_eq!(p1.skus[0].list_price, Some(4.80));
        assert_eq!(p1.skus[0].selling_price, Some(4.20));
        assert_eq!(p1.skus[0].card_price, Some(3.90));
        assert_eq!(p1.payload_hash.len(), 64);

        let p2 = &products[1];
        assert_eq!(p2.skus[0].sku_id, "5001");
        assert_eq!(p2.skus[0].list_price, Some(1599.00));
        assert_eq!(p2.skus[0].selling_price, Some(1299.50));
        assert_eq!(p2.skus[0].card_price, None);

        let p3 = &products[2];
        assert_eq!(p3.skus[0].list_price, Some(4.50));
        assert_eq!(p3.skus[0].selling_price, Some(4.50));
    }

    #[test]
    fn falabella_parses_direct_json_payload() {
        let json = r#"{
          "props": {
            "pageProps": {
              "results": [
                {
                  "productId": "113706444",
                  "skuId": "113706446",
                  "displayName": "Aceite Primor 900ml",
                  "prices": [{"type": "internetPrice", "price": ["8.50"]}]
                }
              ]
            }
          }
        }"#;

        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(json, &ctx).unwrap();
        assert_eq!(products.len(), 1);
        assert_eq!(products[0].skus[0].selling_price, Some(8.50));
    }

    #[test]
    fn falabella_returns_descriptive_error_when_next_data_missing() {
        let blocked_html =
            "<html><title>Attention Required!</title><body>Cloudflare WAF Challenge</body></html>";
        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None).with_endpoint(
            "https://tottus.falabella.com.pe/tottus-pe/search?Ntt=leche&page=1",
            403,
        );

        let err = adapter.parse_page_payload(blocked_html, &ctx).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("__NEXT_DATA__"),
            "expected __NEXT_DATA__ hint in: {msg}"
        );
        assert!(msg.contains("403"), "expected HTTP status 403 in: {msg}");
    }

    #[tokio::test]
    async fn falabella_fetch_page_reads_pagination_for_has_more() {
        let body = r#"<html><script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "pagination": {"count": 40, "perPage": 20, "currentPage": 1, "totalPages": 2},
              "results": [
                {"productId": "10", "skuId": "10", "displayName": "Producto 10", "prices": []}
              ]
            }
          }
        }</script></html>"#;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });

        let adapter = FalabellaListingAdapter::with_base_url(format!("http://{addr}"), test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch.http_status, 200);
        assert_eq!(batch.products.len(), 1);
        assert!(batch.has_more);
        assert_eq!(batch.next_offset, 20);
    }

    #[tokio::test]
    async fn falabella_fetch_page_appends_postal_code_and_store_id_to_endpoint() {
        let body = r#"<html><script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "pagination": {"count": 1, "perPage": 20, "currentPage": 1, "totalPages": 1},
              "results": [
                {"productId": "10", "skuId": "10", "displayName": "Producto 10", "prices": []}
              ]
            }
          }
        }</script></html>"#;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<String>(1);

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 2048];
            let n = socket.read(&mut buf).await.unwrap();
            let req_str = String::from_utf8_lossy(&buf[..n]).to_string();
            let _ = req_tx.send(req_str).await;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });

        let adapter = FalabellaListingAdapter::with_base_url(format!("http://{addr}"), test_http());
        let mut ctx = FetchContext::for_run(Uuid::new_v4(), "PE", Some("15001".to_string()));
        ctx.store_id = Some("TIENDA_1".to_string());
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch.products.len(), 1);

        let req = req_rx.recv().await.unwrap();
        assert!(
            req.contains("postalCode=15001"),
            "request missing postalCode: {req}"
        );
        assert!(
            req.contains("storeId=TIENDA_1"),
            "request missing storeId: {req}"
        );

        let endpoint = &batch.products[0].source_endpoint;
        assert!(
            endpoint.contains("postalCode=15001"),
            "product endpoint missing postalCode: {endpoint}"
        );
        assert!(
            endpoint.contains("storeId=TIENDA_1"),
            "product endpoint missing storeId: {endpoint}"
        );
    }

    #[test]
    fn falabella_parse_price_handles_peruvian_currency_prefixes_commas_and_ranges() {
        assert_eq!(
            parse_price_value(&serde_json::json!("S/. 4.50")),
            Some(4.50)
        );
        assert_eq!(parse_price_value(&serde_json::json!("S/ 4,50")), Some(4.50));
        assert_eq!(
            parse_price_value(&serde_json::json!("S/ 1,299.90")),
            Some(1299.90)
        );
        assert_eq!(
            parse_price_value(&serde_json::json!("S/ 1.299,90")),
            Some(1299.90)
        );
        assert_eq!(
            parse_price_value(&serde_json::json!("S/ 4.50 - S/ 6.00")),
            Some(4.50)
        );
    }

    #[test]
    fn falabella_skips_item_missing_product_id_and_parses_valid_items() {
        let json = r#"{
          "props": {
            "pageProps": {
              "results": [
                {
                  "displayName": "Item without productId",
                  "prices": []
                },
                {
                  "productId": "999",
                  "displayName": "Valid Product",
                  "prices": []
                }
              ]
            }
          }
        }"#;

        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter
            .parse_page_payload(json, &ctx)
            .expect("should succeed");
        assert_eq!(products.len(), 1);
        assert_eq!(products[0].product_id, "999");
    }
}
