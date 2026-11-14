use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::adapter::{FetchContext, PageBatch, RetailerAdapter};
use crate::adapters::{absolutize_url, encode_url_segment, value_as_string};
use crate::dedup::{compute_content_hash, compute_payload_hash};
use crate::domain::{normalize_opt_string, validate_gtin_mod10, AvailabilityObs, Product, SkuItem};
use crate::http::RateLimitedHttpClient;

const TOTTUS_BASE_URL: &str = "https://tottus.falabella.com.pe";
pub const TOTTUS_CACHE_SOURCE_PDP: &str = "tottus_pdp_ssr";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TottusCacheEntry {
    pub ean: Option<String>,
    pub available_qty: Option<i64>,
    pub in_stock: Option<bool>,
    #[serde(default)]
    pub seller_sku_id: Option<String>,
    #[serde(default)]
    pub source: String,
    #[serde(default)]
    pub pdp_checked: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct TottusPdpCache {
    path: Option<PathBuf>,
    entries: BTreeMap<String, TottusCacheEntry>,
    dirty: bool,
}

impl TottusPdpCache {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let raw_path = path.as_ref();
        let normalized_path = if raw_path
            .parent()
            .is_some_and(|parent| parent.as_os_str().is_empty())
        {
            PathBuf::from(".").join(raw_path)
        } else {
            raw_path.to_path_buf()
        };

        let entries = if normalized_path.exists() {
            let bytes = fs::read(&normalized_path).with_context(|| {
                format!(
                    "failed to read Tottus PDP cache at {}",
                    normalized_path.display()
                )
            })?;
            serde_json::from_slice(&bytes).with_context(|| {
                format!(
                    "failed to parse Tottus PDP cache at {}",
                    normalized_path.display()
                )
            })?
        } else {
            BTreeMap::new()
        };

        Ok(Self {
            path: Some(normalized_path),
            entries,
            dirty: false,
        })
    }

    fn in_memory() -> Self {
        Self {
            path: None,
            entries: BTreeMap::new(),
            dirty: false,
        }
    }

    pub fn get(&self, sku_id: &str) -> Option<&TottusCacheEntry> {
        self.entries.get(sku_id)
    }

    pub fn entries(&self) -> &BTreeMap<String, TottusCacheEntry> {
        &self.entries
    }

    pub fn record_pdp_enrichment(&mut self, sku_id: &str, enrichment: &TottusPdpEnrichment) {
        let existing = self.entries.get(sku_id);
        let existing_valid_ean = existing
            .filter(|e| e.source == TOTTUS_CACHE_SOURCE_PDP)
            .and_then(|e| e.ean.as_deref())
            .and_then(validate_gtin_mod10);
        let merged_ean = enrichment
            .ean
            .as_deref()
            .and_then(validate_gtin_mod10)
            .or(existing_valid_ean);

        let entry = TottusCacheEntry {
            ean: merged_ean,
            available_qty: enrichment
                .available_qty
                .or_else(|| existing.and_then(|e| e.available_qty)),
            in_stock: enrichment
                .in_stock
                .or_else(|| existing.and_then(|e| e.in_stock)),
            seller_sku_id: enrichment
                .seller_sku_id
                .clone()
                .or_else(|| existing.and_then(|e| e.seller_sku_id.clone())),
            source: TOTTUS_CACHE_SOURCE_PDP.to_string(),
            pdp_checked: true,
            updated_at: Utc::now(),
        };
        self.entries.insert(sku_id.to_string(), entry);
        self.dirty = true;
    }

    pub fn persist_if_dirty(&mut self) -> anyhow::Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let Some(path) = &self.path else {
            self.dirty = false;
            return Ok(());
        };
        let payload = serde_json::to_vec_pretty(&self.entries)?;
        crate::atomic_write(path, &payload)?;
        self.dirty = false;
        Ok(())
    }
}

pub struct FalabellaListingAdapter {
    base_url: String,
    http: Arc<RateLimitedHttpClient>,
    enrich_pdp: bool,
    cache_path: Option<PathBuf>,
    cache: Mutex<Option<TottusPdpCache>>,
}

impl FalabellaListingAdapter {
    pub fn new_tottus(http: Arc<RateLimitedHttpClient>) -> Self {
        Self::with_enrichment(TOTTUS_BASE_URL, http, false, None)
    }

    pub fn with_base_url(base_url: impl Into<String>, http: Arc<RateLimitedHttpClient>) -> Self {
        Self::with_enrichment(base_url, http, false, None)
    }

    pub fn new_tottus_with_enrichment(
        http: Arc<RateLimitedHttpClient>,
        enrich_pdp: bool,
        cache_path: Option<PathBuf>,
    ) -> Self {
        Self::with_enrichment(TOTTUS_BASE_URL, http, enrich_pdp, cache_path)
    }

    pub fn with_enrichment(
        base_url: impl Into<String>,
        http: Arc<RateLimitedHttpClient>,
        enrich_pdp: bool,
        cache_path: Option<PathBuf>,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
            enrich_pdp,
            cache_path,
            cache: Mutex::new(None),
        }
    }

    async fn apply_cache_and_collect_pdp_targets(
        &self,
        products: &mut [Product],
    ) -> anyhow::Result<Vec<(String, String)>> {
        let mut guard = self.cache.lock().await;
        if guard.is_none() {
            let loaded = match &self.cache_path {
                Some(path) => TottusPdpCache::load(path)?,
                None if self.enrich_pdp => TottusPdpCache::in_memory(),
                None => return Ok(Vec::new()),
            };
            *guard = Some(loaded);
        }

        let cache = guard.as_ref().expect("cache initialized above");
        let mut to_fetch = Vec::new();
        let mut queued_skus = HashSet::new();

        for product in products.iter_mut() {
            for sku in &mut product.skus {
                let cache_entry = cache.get(&sku.sku_id);
                if let Some(entry) =
                    cache_entry.filter(|entry| entry.source == TOTTUS_CACHE_SOURCE_PDP)
                {
                    if sku.ean.is_none() {
                        if let Some(ean) = entry.ean.as_deref().and_then(validate_gtin_mod10) {
                            sku.ean = Some(ean);
                            sku.ean_source = Some("pdp".to_string());
                        }
                    }
                }

                if self.enrich_pdp
                    && sku.ean.is_none()
                    && cache_entry
                        .is_none_or(|e| !e.pdp_checked || e.source != TOTTUS_CACHE_SOURCE_PDP)
                {
                    if let Some(url) = product.product_url.as_ref() {
                        if queued_skus.insert(sku.sku_id.clone()) {
                            to_fetch.push((sku.sku_id.clone(), url.clone()));
                        }
                    }
                }
            }
        }

        Ok(to_fetch)
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

fn is_thousands_grouped(s: &str, sep: char) -> bool {
    let mut parts = s.split(sep);
    let Some(head) = parts.next() else {
        return false;
    };
    if head.is_empty()
        || head.len() > 3
        || head.starts_with('0')
        || !head.bytes().all(|b| b.is_ascii_digit())
    {
        return false;
    }
    let mut has_tail = false;
    for part in parts {
        if part.len() != 3 || !part.bytes().all(|b| b.is_ascii_digit()) {
            return false;
        }
        has_tail = true;
    }
    has_tail
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
        if is_thousands_grouped(&filtered, ',') {
            filtered.replace(',', "")
        } else {
            filtered.replace(',', ".")
        }
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

fn extract_falabella_prices(
    item: &Value,
) -> (Option<f64>, Option<f64>, Option<f64>, Option<String>) {
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
    let card_program = card_price.map(|_| "CMR".to_string());

    (list_price, selling_price, card_price, card_program)
}

fn extract_gtin_from_value(val: &Value) -> Option<String> {
    match val {
        Value::String(s) => validate_gtin_mod10(s),
        Value::Number(n) => validate_gtin_mod10(&n.to_string()),
        Value::Array(arr) => arr.iter().find_map(extract_gtin_from_value),
        _ => None,
    }
}

fn spec_key_mentions_gtin(key: &str) -> bool {
    let lower = key.to_lowercase();
    lower.contains("ean")
        || lower.contains("gtin")
        || lower.contains("código de barras")
        || lower.contains("codigo de barras")
}

fn extract_gtin_from_specs(specs: &Value) -> Option<String> {
    match specs {
        Value::Array(entries) => entries.iter().find_map(|entry| {
            let mentions_gtin = ["name", "id", "label", "key"]
                .iter()
                .filter_map(|field| entry.get(field).and_then(|v| v.as_str()))
                .any(spec_key_mentions_gtin);
            if !mentions_gtin {
                return None;
            }
            ["value", "values", "val"]
                .iter()
                .filter_map(|field| entry.get(field))
                .find_map(extract_gtin_from_value)
        }),
        Value::Object(map) => map.iter().find_map(|(key, val)| {
            if spec_key_mentions_gtin(key) {
                extract_gtin_from_value(val)
            } else {
                None
            }
        }),
        _ => None,
    }
}

pub fn extract_tottus_listing_ean(item: &Value) -> Option<String> {
    for field in ["ean", "gtin13", "gtin", "barcode"] {
        if let Some(ean) = item.get(field).and_then(extract_gtin_from_value) {
            return Some(ean);
        }
    }
    for spec_field in ["topSpecifications", "specifications"] {
        if let Some(ean) = item.get(spec_field).and_then(extract_gtin_from_specs) {
            return Some(ean);
        }
    }
    None
}

fn parse_non_negative_i64(val: &Value) -> Option<i64> {
    val.as_i64()
        .filter(|&n| n >= 0)
        .or_else(|| val.as_u64().and_then(|u| i64::try_from(u).ok()))
        .or_else(|| {
            val.as_str()
                .and_then(|s| s.trim().parse::<i64>().ok())
                .filter(|&n| n >= 0)
        })
}

pub fn extract_tottus_listing_availability(item: &Value) -> AvailabilityObs {
    let out_of_stock = item.get("isOutOfStock").and_then(|v| v.as_bool()) == Some(true)
        || item
            .pointer("/availability/isOutOfStock")
            .and_then(|v| v.as_bool())
            == Some(true);
    if out_of_stock {
        return AvailabilityObs {
            qty: Some(0),
            status: "out_of_stock",
            source: "tottus_listing_flag",
        };
    }

    for field in ["availableQuantity", "stock"] {
        if let Some(qty) = item.get(field).and_then(parse_non_negative_i64) {
            return AvailabilityObs {
                qty: Some(qty),
                status: if qty > 0 { "in_stock" } else { "out_of_stock" },
                source: "tottus_listing_qty",
            };
        }
    }

    AvailabilityObs {
        qty: None,
        status: "listed_unknown",
        source: "tottus_listing_presence",
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TottusPdpEnrichment {
    pub ean: Option<String>,
    pub available_qty: Option<i64>,
    pub in_stock: Option<bool>,
    pub seller_sku_id: Option<String>,
}

fn parse_positive_i64(val: &Value) -> Option<i64> {
    parse_non_negative_i64(val).filter(|&n| n > 0)
}

fn extract_json_ld_blocks(html: &str) -> Vec<Value> {
    let lower = html.to_ascii_lowercase();
    let mut blocks = Vec::new();
    let mut cursor = 0usize;

    while let Some(rel_start) = lower[cursor..].find("<script") {
        let tag_start = cursor + rel_start;
        let Some(rel_gt) = lower[tag_start..].find('>') else {
            break;
        };
        let tag_end = tag_start + rel_gt;
        let attrs = &lower[tag_start..=tag_end];
        let body_start = tag_end + 1;
        let Some(rel_close) = lower[body_start..].find("</script>") else {
            break;
        };
        let body_end = body_start + rel_close;

        if attrs.contains("application/ld+json") {
            let raw_json = html[body_start..body_end].trim();
            if let Ok(val) = serde_json::from_str::<Value>(raw_json) {
                blocks.push(val);
            }
        }

        cursor = body_end + "</script>".len();
    }

    blocks
}

fn is_json_ld_product(obj: &serde_json::Map<String, Value>) -> bool {
    match obj.get("@type") {
        Some(Value::String(s)) => s.eq_ignore_ascii_case("product"),
        Some(Value::Array(arr)) => arr.iter().any(|v| {
            v.as_str()
                .is_some_and(|s| s.eq_ignore_ascii_case("product"))
        }),
        _ => false,
    }
}

fn collect_json_ld_products<'a>(val: &'a Value, out: &mut Vec<&'a Value>) {
    match val {
        Value::Array(arr) => {
            for item in arr {
                collect_json_ld_products(item, out);
            }
        }
        Value::Object(map) => {
            if is_json_ld_product(map) {
                out.push(val);
            }
            if let Some(graph) = map.get("@graph") {
                collect_json_ld_products(graph, out);
            }
        }
        _ => {}
    }
}

fn parse_json_ld_availability(offers: &Value) -> Option<bool> {
    let mut saw_oos = false;
    let slice: &[Value] = match offers {
        Value::Array(arr) => arr.as_slice(),
        single @ Value::Object(_) => std::slice::from_ref(single),
        _ => return None,
    };

    for offer in slice {
        if let Some(avail) = offer.get("availability").and_then(|v| v.as_str()) {
            let lower = avail.to_ascii_lowercase();
            if lower.contains("outofstock") {
                saw_oos = true;
            } else if lower.contains("instock") {
                return Some(true);
            }
        }
    }

    if saw_oos {
        Some(false)
    } else {
        None
    }
}

fn extract_variant_stock_limit(variant: &Value) -> Option<i64> {
    variant
        .pointer("/qtyLimits/value")
        .and_then(parse_positive_i64)
        .or_else(|| {
            variant
                .get("offerings")
                .and_then(|o| o.as_array())
                .and_then(|arr| {
                    arr.iter().find_map(|off| {
                        off.pointer("/qtyLimits/value").and_then(parse_positive_i64)
                    })
                })
        })
}

pub fn parse_tottus_pdp_html(html: &str, target_sku_id: &str) -> TottusPdpEnrichment {
    let json_ld_blocks = extract_json_ld_blocks(html);
    let mut ld_products = Vec::new();
    for block in &json_ld_blocks {
        collect_json_ld_products(block, &mut ld_products);
    }

    let mut jsonld_ean: Option<String> = None;
    let mut jsonld_in_stock: Option<bool> = None;

    for prod in &ld_products {
        if jsonld_ean.is_none() {
            for field in ["gtin13", "gtin", "gtin12", "gtin8", "gtin14", "ean"] {
                if let Some(ean) = prod.get(field).and_then(extract_gtin_from_value) {
                    jsonld_ean = Some(ean);
                    break;
                }
            }
        }
        if jsonld_in_stock.is_none() {
            if let Some(offers) = prod.get("offers") {
                jsonld_in_stock = parse_json_ld_availability(offers);
            }
        }
    }

    let next_root = extract_next_data_json(html, 200).ok();
    let product_data = next_root
        .as_ref()
        .and_then(|r| r.pointer("/props/pageProps/productData"));
    let variants = product_data
        .and_then(|pd| pd.get("variants"))
        .and_then(|v| v.as_array());
    let matched_variant = variants.and_then(|arr| {
        arr.iter()
            .find(|v| {
                value_as_string(v.get("id")).as_deref() == Some(target_sku_id)
                    || value_as_string(v.get("skuId")).as_deref() == Some(target_sku_id)
            })
            .or_else(|| arr.first())
    });

    let next_is_oos = matched_variant
        .and_then(|v| v.get("isOutOfStock").and_then(|b| b.as_bool()))
        .or_else(|| product_data.and_then(|pd| pd.get("isOutOfStock").and_then(|b| b.as_bool())));

    let in_stock = match next_is_oos {
        Some(true) => Some(false),
        Some(false) => Some(true),
        None => jsonld_in_stock,
    };

    let available_qty = if in_stock == Some(false) {
        Some(0)
    } else {
        matched_variant
            .and_then(extract_variant_stock_limit)
            .or_else(|| {
                product_data.and_then(|pd| {
                    pd.pointer("/qtyLimits/value")
                        .and_then(parse_positive_i64)
                        .or_else(|| {
                            let stock_units = pd.get("stockUnits")?;
                            let variant_id = matched_variant.and_then(|v| {
                                value_as_string(v.get("id"))
                                    .or_else(|| value_as_string(v.get("skuId")))
                            });
                            stock_units
                                .get(target_sku_id)
                                .and_then(parse_positive_i64)
                                .or_else(|| {
                                    variant_id
                                        .as_deref()
                                        .and_then(|id| stock_units.get(id))
                                        .and_then(parse_positive_i64)
                                })
                        })
                })
            })
    };

    let seller_sku_id = matched_variant
        .and_then(|v| v.get("offerings"))
        .and_then(|o| o.as_array())
        .and_then(|arr| {
            arr.iter()
                .find_map(|off| value_as_string(off.get("sellerSkuId")))
        });

    let ean = jsonld_ean
        .or_else(|| {
            matched_variant
                .and_then(|v| v.pointer("/attributes/specifications"))
                .and_then(extract_gtin_from_specs)
        })
        .or_else(|| {
            product_data
                .and_then(|pd| pd.pointer("/attributes/specifications"))
                .and_then(extract_gtin_from_specs)
        })
        .or_else(|| {
            variants.and_then(|arr| {
                arr.iter().find_map(|v| {
                    v.pointer("/attributes/specifications")
                        .and_then(extract_gtin_from_specs)
                })
            })
        })
        .or_else(|| matched_variant.and_then(extract_tottus_listing_ean))
        .or_else(|| product_data.and_then(extract_tottus_listing_ean));

    TottusPdpEnrichment {
        ean,
        available_qty,
        in_stock,
        seller_sku_id,
    }
}

fn parse_results_from_root(
    root: &Value,
    base_url: &str,
    extractor_version: &str,
    volatile_keys: &[&str],
    ctx: &FetchContext,
) -> anyhow::Result<(Vec<Product>, usize)> {
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
    let mut rejected = 0usize;
    for item in results {
        let Some(product_id) = value_as_string(item.get("productId")) else {
            rejected += 1;
            tracing::warn!(
                http_status = ctx.http_status,
                "Falabella/Tottus result item missing productId; skipping item"
            );
            continue;
        };
        let raw_payload = serde_json::to_string(item)?;
        let payload_hash = compute_payload_hash(&raw_payload);
        let content_hash = compute_content_hash(&raw_payload, volatile_keys);
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
        let (list_price, selling_price, card_price, card_program) = extract_falabella_prices(item);
        let ean = extract_tottus_listing_ean(item);
        let ean_source = ean.as_ref().map(|_| "listing".to_string());
        let availability = extract_tottus_listing_availability(item);

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
            query_term: ctx.query_term.clone(),
            content_hash,
            skus: vec![SkuItem {
                sku_id,
                ean,
                ean_source,
                availability_status: availability.status.to_string(),
                availability_source: availability.source.to_string(),
                seller_id,
                seller_name: None,
                seller_count: None,
                is_default_seller: None,
                measurement_unit: None,
                unit_multiplier: None,
                raw_name,
                image_url,
                list_price,
                selling_price,
                card_price,
                card_program,
                available_qty: availability.qty,
            }],
        });
    }

    Ok((products, rejected))
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
        concat!(
            "falabella-listing-v",
            env!("CARGO_PKG_VERSION"),
            "+",
            env!("INGESTOR_GIT_SHA")
        )
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
        parse_results_from_root(
            &root,
            &self.base_url,
            self.extractor_version(),
            self.volatile_payload_keys(),
            ctx,
        )
        .map(|(products, _)| products)
    }

    async fn fetch_page(
        &self,
        category_or_query: &str,
        offset: usize,
        page_size: usize,
        ctx: &FetchContext,
    ) -> anyhow::Result<PageBatch> {
        let url = self.build_page_url(category_or_query, offset, page_size);
        let resp = self.http.get_text(&url).await?;
        let page_ctx = ctx.clone().with_endpoint(url, resp.status);
        let root = extract_next_data_json(&resp.body, resp.status)?;
        let (mut products, products_rejected) = parse_results_from_root(
            &root,
            &self.base_url,
            self.extractor_version(),
            self.volatile_payload_keys(),
            &page_ctx,
        )?;
        let has_more = has_more_pages(&root, products.len() + products_rejected, page_size);
        let mut total_bytes = resp.bytes;

        let to_fetch = self
            .apply_cache_and_collect_pdp_targets(&mut products)
            .await?;
        if !to_fetch.is_empty() {
            let mut fetched: HashMap<String, TottusPdpEnrichment> = HashMap::new();
            for (sku_id, pdp_url) in to_fetch {
                match self.http.get_text(&pdp_url).await {
                    Ok(pdp_resp) => {
                        total_bytes += pdp_resp.bytes;
                        let enrichment = parse_tottus_pdp_html(&pdp_resp.body, &sku_id);
                        fetched.insert(sku_id, enrichment);
                    }
                    Err(err) => {
                        tracing::warn!(
                            sku_id = %sku_id,
                            url = %pdp_url,
                            error = %format!("{err:#}"),
                            "Failed fetching Tottus PDP for enrichment; continuing listing batch"
                        );
                    }
                }
            }

            if !fetched.is_empty() {
                for product in &mut products {
                    for sku in &mut product.skus {
                        if let Some(enrichment) = fetched.get(&sku.sku_id) {
                            if sku.ean.is_none() {
                                if let Some(ean) =
                                    enrichment.ean.as_deref().and_then(validate_gtin_mod10)
                                {
                                    sku.ean = Some(ean);
                                    sku.ean_source = Some("pdp".to_string());
                                }
                            }
                            if let Some(in_stock) = enrichment.in_stock {
                                sku.available_qty = if !in_stock {
                                    Some(0)
                                } else {
                                    enrichment.available_qty
                                };
                                sku.availability_status = if in_stock {
                                    "in_stock".to_string()
                                } else {
                                    "out_of_stock".to_string()
                                };
                                sku.availability_source = "tottus_pdp".to_string();
                            }
                        }
                    }
                }

                let mut guard = self.cache.lock().await;
                if let Some(cache) = guard.as_mut() {
                    for (sku_id, enrichment) in &fetched {
                        cache.record_pdp_enrichment(sku_id, enrichment);
                    }
                    cache.persist_if_dirty()?;
                }
            }
        }

        Ok(PageBatch {
            products,
            has_more,
            next_offset: offset + page_size.max(1),
            raw_bytes: total_bytes,
            http_status: resp.status,
            latency_ms: resp.latency_ms,
            products_rejected,
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

    #[test]
    fn falabella_sets_card_program_cmr_and_listing_stock_signals() {
        let html = r#"<!doctype html><html><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "results": [
                {
                  "productId": "101",
                  "skuId": "1011",
                  "displayName": "Leche con CMR y oferta activa",
                  "offeringId": "off-1011",
                  "prices": [
                    {"type": "normalPrice", "price": ["5.00"]},
                    {"type": "internetPrice", "price": ["4.50"]},
                    {"type": "cmrPrice", "price": ["3.90"]}
                  ]
                },
                {
                  "productId": "102",
                  "skuId": "1021",
                  "displayName": "Leche sin stock",
                  "isOutOfStock": true,
                  "maxOnCartAmount": 24,
                  "offeringId": "off-1021",
                  "prices": [
                    {"type": "internetPrice", "price": ["4.50"]}
                  ]
                },
                {
                  "productId": "103",
                  "skuId": "1031",
                  "displayName": "Leche con stock numerico",
                  "availableQuantity": 18,
                  "prices": [
                    {"type": "internetPrice", "price": ["4.50"]}
                  ]
                },
                {
                  "productId": "104",
                  "skuId": "1041",
                  "displayName": "Leche OOS en availability",
                  "availability": {"isOutOfStock": true},
                  "prices": [
                    {"type": "internetPrice", "price": ["4.50"]}
                  ]
                }
              ]
            }
          }
        }</script></body></html>"#;

        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(html, &ctx).unwrap();
        assert_eq!(products.len(), 4);

        assert_eq!(products[0].skus[0].card_price, Some(3.90));
        assert_eq!(products[0].skus[0].card_program.as_deref(), Some("CMR"));
        assert_eq!(products[0].skus[0].available_qty, None);
        assert_eq!(products[0].skus[0].availability_status, "listed_unknown");
        assert_eq!(
            products[0].skus[0].availability_source,
            "tottus_listing_presence"
        );

        assert_eq!(products[1].skus[0].card_price, None);
        assert_eq!(products[1].skus[0].card_program, None);
        assert_eq!(products[1].skus[0].available_qty, Some(0));
        assert_eq!(products[1].skus[0].availability_status, "out_of_stock");
        assert_eq!(
            products[1].skus[0].availability_source,
            "tottus_listing_flag"
        );

        assert_eq!(products[2].skus[0].available_qty, Some(18));
        assert_eq!(products[2].skus[0].availability_status, "in_stock");
        assert_eq!(
            products[2].skus[0].availability_source,
            "tottus_listing_qty"
        );
        assert_eq!(products[3].skus[0].available_qty, Some(0));
        assert_eq!(products[3].skus[0].availability_status, "out_of_stock");
        assert_eq!(
            products[3].skus[0].availability_source,
            "tottus_listing_flag"
        );

        let no_signal = serde_json::json!({"productId": "105", "prices": []});
        assert_eq!(
            extract_tottus_listing_availability(&no_signal),
            AvailabilityObs {
                qty: None,
                status: "listed_unknown",
                source: "tottus_listing_presence",
            }
        );
        let null_offer =
            serde_json::json!({"productId": "106", "offeringId": null, "availability": null});
        assert_eq!(
            extract_tottus_listing_availability(&null_offer),
            AvailabilityObs {
                qty: None,
                status: "listed_unknown",
                source: "tottus_listing_presence",
            }
        );
        let with_avail =
            serde_json::json!({"productId": "107", "availability": {"homeDelivery": "AVAILABLE"}});
        assert_eq!(
            extract_tottus_listing_availability(&with_avail),
            AvailabilityObs {
                qty: None,
                status: "listed_unknown",
                source: "tottus_listing_presence",
            }
        );
        let with_stock_field = serde_json::json!({"productId": "108", "stock": 7});
        assert_eq!(
            extract_tottus_listing_availability(&with_stock_field),
            AvailabilityObs {
                qty: Some(7),
                status: "in_stock",
                source: "tottus_listing_qty",
            }
        );
        let with_zero_stock = serde_json::json!({"productId": "108b", "stock": 0});
        assert_eq!(
            extract_tottus_listing_availability(&with_zero_stock),
            AvailabilityObs {
                qty: Some(0),
                status: "out_of_stock",
                source: "tottus_listing_qty",
            }
        );
        let with_max_cart = serde_json::json!({"productId": "109", "maxOnCartAmount": 12});
        assert_eq!(
            extract_tottus_listing_availability(&with_max_cart),
            AvailabilityObs {
                qty: None,
                status: "listed_unknown",
                source: "tottus_listing_presence",
            }
        );
    }

    #[tokio::test]
    async fn falabella_fetch_page_does_not_send_unverified_geo_params() {
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
            !req.contains("postalCode="),
            "request unexpectedly sent postalCode: {req}"
        );
        assert!(
            !req.contains("storeId="),
            "request unexpectedly sent storeId: {req}"
        );

        let endpoint = &batch.products[0].source_endpoint;
        assert!(
            !endpoint.contains("postalCode="),
            "product endpoint unexpectedly sent postalCode: {endpoint}"
        );
        assert!(
            !endpoint.contains("storeId="),
            "product endpoint unexpectedly sent storeId: {endpoint}"
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
        assert_eq!(parse_price_value(&serde_json::json!("1,299")), Some(1299.0));
        assert_eq!(
            parse_price_value(&serde_json::json!("S/ 1,299")),
            Some(1299.0)
        );
        assert_eq!(
            parse_price_value(&serde_json::json!("12,999")),
            Some(12999.0)
        );
        assert_eq!(
            parse_price_value(&serde_json::json!("1,299,000")),
            Some(1_299_000.0)
        );
        assert_eq!(parse_price_value(&serde_json::json!("1,5")), Some(1.5));
        assert_eq!(parse_price_value(&serde_json::json!("27")), Some(27.0));
        assert_eq!(parse_price_value(&serde_json::json!("0,500")), Some(0.5));
        assert_eq!(
            parse_price_value(&serde_json::json!("S/ 0,990")),
            Some(0.99)
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

        let root: Value = serde_json::from_str(json).unwrap();
        let (parsed, rejected) = parse_results_from_root(
            &root,
            &adapter.base_url,
            adapter.extractor_version(),
            adapter.volatile_payload_keys(),
            &ctx,
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(rejected, 1);
    }

    #[test]
    fn falabella_extracts_tottus_listing_ean_from_fields_and_specifications() {
        let direct_gtin = serde_json::json!({
            "ean": "null",
            "gtin13": "7751271034142"
        });
        assert_eq!(
            extract_tottus_listing_ean(&direct_gtin),
            Some("7751271034142".to_string())
        );

        let array_barcode = serde_json::json!({
            "barcode": ["12345", "7751271034098"]
        });
        assert_eq!(
            extract_tottus_listing_ean(&array_barcode),
            Some("7751271034098".to_string())
        );

        let top_specs = serde_json::json!({
            "topSpecifications": [
                {"name": "Marca", "value": "GLORIA"},
                {"name": "Código de barras", "value": "7751271033664"}
            ]
        });
        assert_eq!(
            extract_tottus_listing_ean(&top_specs),
            Some("7751271033664".to_string())
        );

        let specs_by_id = serde_json::json!({
            "specifications": [
                {"id": "registro_sanitario", "value": "20181512"},
                {"id": "gtin", "values": ["invalid", "7751271034128"]}
            ]
        });
        assert_eq!(
            extract_tottus_listing_ean(&specs_by_id),
            Some("7751271034128".to_string())
        );

        let invalid_only = serde_json::json!({
            "ean": "7751271034143",
            "gtin": "null"
        });
        assert_eq!(extract_tottus_listing_ean(&invalid_only), None);

        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let listing_payload = r#"{"props":{"pageProps":{"results":[
          {"productId":"P1","skuId":"S1","displayName":"Leche Con EAN","gtin13":"7751271034142","prices":[]},
          {"productId":"P2","skuId":"S2","displayName":"Leche Sin EAN","prices":[]}
        ]}}}"#;
        let products = adapter.parse_page_payload(listing_payload, &ctx).unwrap();
        assert_eq!(products[0].skus[0].ean.as_deref(), Some("7751271034142"));
        assert_eq!(products[0].skus[0].ean_source.as_deref(), Some("listing"));
        assert_eq!(products[1].skus[0].ean, None);
        assert_eq!(products[1].skus[0].ean_source, None);
    }

    #[test]
    fn parse_tottus_pdp_html_extracts_json_ld_gtin_and_next_data_stock_and_seller_sku() {
        let html = r#"<!doctype html><html><head>
        <script type="application/ld+json">{
          "@context": "https://schema.org",
          "@type": "BreadcrumbList",
          "itemListElement": []
        }</script>
        <script type="application/ld+json">{
          "@context": "https://schema.org",
          "@type": "Product",
          "name": "Leche Gloria Light",
          "gtin13": "7751271034142",
          "offers": [
            {"@type": "Offer", "availability": "https://schema.org/InStock", "price": "24.90"}
          ]
        }</script>
        </head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "productData": {
                "isOutOfStock": false,
                "stockUnits": {"129087939": 5, "129087940": 0},
                "variants": [
                  {
                    "id": "129087939",
                    "maxOnCartAmount": 6,
                    "offerings": [{"sellerSkuId": "SELL-939"}]
                  },
                  {
                    "id": "129087940",
                    "maxOnCartAmount": 24,
                    "offerings": [{"sellerSkuId": "SELL-940"}]
                  }
                ]
              }
            }
          }
        }</script>
        </body></html>"#;

        let parsed = parse_tottus_pdp_html(html, "129087940");
        assert_eq!(
            parsed,
            TottusPdpEnrichment {
                ean: Some("7751271034142".to_string()),
                available_qty: None,
                in_stock: Some(true),
                seller_sku_id: Some("SELL-940".to_string()),
            }
        );
    }

    #[test]
    fn parse_tottus_pdp_html_handles_graph_json_ld_spec_fallback_and_qty_limits() {
        let html_graph_qty_limit = r#"<html><head>
        <script type='application/ld+json'>{
          "@context": "https://schema.org",
          "@graph": [
            {"@type": "WebPage"},
            {
              "@type": "Product",
              "gtin13": "null",
              "offers": {"@type": "Offer", "availability": "https://schema.org/InStock"}
            }
          ]
        }</script></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "productData": {
                "isOutOfStock": false,
                "attributes": {
                  "specifications": [{"name": "Código de barras", "value": "7751271034098"}]
                },
                "variants": [
                  {
                    "id": "FALLBACK-SKU",
                    "qtyLimits": {"value": 12},
                    "offerings": [{"sellerSkuId": "SELLER-1"}]
                  }
                ]
              }
            }
          }
        }</script></body></html>"#;

        let parsed = parse_tottus_pdp_html(html_graph_qty_limit, "MISSING-SKU");
        assert_eq!(parsed.ean.as_deref(), Some("7751271034098"));
        assert_eq!(parsed.in_stock, Some(true));
        assert_eq!(parsed.available_qty, Some(12));
        assert_eq!(parsed.seller_sku_id.as_deref(), Some("SELLER-1"));

        let html_stock_units = r#"<html><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": false,
            "stockUnits": {"555": 9},
            "variants": [{
              "id": "555",
              "attributes": {"specifications": [{"id": "ean", "value": "7751271033664"}]}
            }]
          }}}
        }</script></body></html>"#;
        let parsed_units = parse_tottus_pdp_html(html_stock_units, "555");
        assert_eq!(parsed_units.ean.as_deref(), Some("7751271033664"));
        assert_eq!(parsed_units.in_stock, Some(true));
        assert_eq!(parsed_units.available_qty, Some(9));

        let html_instock_zero_units_no_limit = r#"<html><body>
        <script type="application/ld+json">[{"@type": "Product", "offers": {"availability": "InStock"}}]</script>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": false,
            "stockUnits": {"777": 0},
            "variants": [{"id": "777"}]
          }}}
        }</script></body></html>"#;
        let parsed_no_fabricated_qty =
            parse_tottus_pdp_html(html_instock_zero_units_no_limit, "777");
        assert_eq!(parsed_no_fabricated_qty.in_stock, Some(true));
        assert_eq!(parsed_no_fabricated_qty.available_qty, None);
    }

    #[test]
    fn parse_tottus_pdp_html_rejects_literal_null_gtin_while_preserving_stock() {
        let html = r#"<!doctype html><html><head>
        <script type="application/ld+json">{
          "@context": "https://schema.org",
          "@type": "Product",
          "gtin13": "null",
          "gtin": "null",
          "offers": {"@type": "Offer", "availability": "https://schema.org/InStock"}
        }</script>
        </head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "productData": {
                "isOutOfStock": false,
                "stockUnits": {"113705686": 0},
                "variants": [
                  {
                    "id": "113705686",
                    "maxOnCartAmount": 24,
                    "offerings": [{"sellerSkuId": "20456789"}]
                  }
                ]
              }
            }
          }
        }</script>
        </body></html>"#;

        let parsed = parse_tottus_pdp_html(html, "113705686");
        assert_eq!(parsed.ean, None);
        assert_eq!(parsed.in_stock, Some(true));
        assert_eq!(parsed.available_qty, None);
        assert_eq!(parsed.seller_sku_id.as_deref(), Some("20456789"));
    }

    #[test]
    fn parse_tottus_pdp_html_returns_zero_qty_when_out_of_stock() {
        let html_both_oos = r#"<html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "gtin13": "7751271034142",
          "offers": {"availability": "https://schema.org/OutOfStock"}
        }</script></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": true,
            "variants": [{"id": "888", "maxOnCartAmount": 24}]
          }}}
        }</script></body></html>"#;
        let parsed = parse_tottus_pdp_html(html_both_oos, "888");
        assert_eq!(parsed.ean.as_deref(), Some("7751271034142"));
        assert_eq!(parsed.in_stock, Some(false));
        assert_eq!(parsed.available_qty, Some(0));

        let html_jsonld_oos_only = r#"<html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "offers": [{"availability": "https://schema.org/OutOfStock"}]
        }</script></head></html>"#;
        let parsed_ld = parse_tottus_pdp_html(html_jsonld_oos_only, "888");
        assert_eq!(parsed_ld.in_stock, Some(false));
        assert_eq!(parsed_ld.available_qty, Some(0));
    }

    #[test]
    fn tottus_pdp_cache_preserves_valid_ean_when_pdp_returns_null_and_persists_atomically() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut initial = std::collections::BTreeMap::new();
        initial.insert(
            "129087925".to_string(),
            TottusCacheEntry {
                ean: Some("7751271034098".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "tottus_pdp_ssr".to_string(),
                pdp_checked: true,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&initial).unwrap()).unwrap();

        let mut cache = TottusPdpCache::load(&cache_path).unwrap();
        let pdp_null_ean = TottusPdpEnrichment {
            ean: None,
            available_qty: Some(24),
            in_stock: Some(true),
            seller_sku_id: Some("SELL-129".to_string()),
        };
        cache.record_pdp_enrichment("129087925", &pdp_null_ean);
        cache.persist_if_dirty().unwrap();

        let reloaded = TottusPdpCache::load(&cache_path).unwrap();
        let entry = reloaded.get("129087925").unwrap();
        assert_eq!(entry.ean.as_deref(), Some("7751271034098"));
        assert_eq!(entry.source, "tottus_pdp_ssr");
        assert_eq!(entry.available_qty, Some(24));
        assert_eq!(entry.in_stock, Some(true));
        assert_eq!(entry.seller_sku_id.as_deref(), Some("SELL-129"));
        assert!(entry.pdp_checked);
    }

    #[tokio::test]
    async fn falabella_fetch_page_enriches_pdp_only_for_uncached_skus_and_caches_results() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut seed = std::collections::BTreeMap::new();
        seed.insert(
            "1001".to_string(),
            TottusCacheEntry {
                ean: Some("7751271034098".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "vtex_crossref_gtin13_seed".to_string(),
                pdp_checked: false,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&seed).unwrap()).unwrap();

        let listing_body = r#"<!doctype html><html><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "pagination": {"count": 3, "perPage": 20, "currentPage": 1, "totalPages": 1},
              "results": [
                {
                  "productId": "P1001",
                  "skuId": "1001",
                  "displayName": "Leche Preseeded",
                  "url": "/tottus-pe/product/P1001/leche-preseeded/1001",
                  "offeringId": "off-1001",
                  "prices": [{"type": "internetPrice", "price": ["24.90"]}]
                },
                {
                  "productId": "P1002",
                  "skuId": "1002",
                  "displayName": "Leche Null Gtin",
                  "url": "/tottus-pe/product/P1002/leche-null-gtin/1002",
                  "offeringId": "off-1002",
                  "prices": [
                    {"type": "internetPrice", "price": ["18.50"]},
                    {"type": "cmrPrice", "price": ["16.90"]}
                  ]
                },
                {
                  "productId": "P1003",
                  "skuId": "1003",
                  "displayName": "Arroz Con Gtin OOS",
                  "url": "/tottus-pe/product/P1003/arroz-con-gtin/1003",
                  "offeringId": "off-1003",
                  "prices": [{"type": "normalPrice", "price": ["21.00"]}]
                }
              ]
            }
          }
        }</script></body></html>"#
            .to_string();

        let pdp_1001_body = "<html></html>".to_string();
        let pdp_1002_body = r#"<!doctype html><html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "gtin13": "null",
          "offers": {"@type": "Offer", "availability": "https://schema.org/InStock"}
        }</script></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": false,
            "stockUnits": {"1002": 0},
            "variants": [{"id": "1002", "maxOnCartAmount": 24, "offerings": [{"sellerSkuId": "S1002"}]}]
          }}}
        }</script></body></html>"#
            .to_string();

        let pdp_1003_body = r#"<!doctype html><html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "gtin13": "7751271034142",
          "offers": {"@type": "Offer", "availability": "https://schema.org/OutOfStock"}
        }</script></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": true,
            "variants": [{"id": "1003", "maxOnCartAmount": 12, "offerings": [{"sellerSkuId": "S1003"}]}]
          }}}
        }</script></body></html>"#
            .to_string();

        let expected_batch1_bytes =
            listing_body.len() + pdp_1001_body.len() + pdp_1002_body.len() + pdp_1003_body.len();
        let expected_listing_bytes = listing_body.len();

        let search_hits = Arc::new(AtomicUsize::new(0));
        let pdp_1001_hits = Arc::new(AtomicUsize::new(0));
        let pdp_1002_hits = Arc::new(AtomicUsize::new(0));
        let pdp_1003_hits = Arc::new(AtomicUsize::new(0));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let search_hits_srv = search_hits.clone();
        let pdp_1001_hits_srv = pdp_1001_hits.clone();
        let pdp_1002_hits_srv = pdp_1002_hits.clone();
        let pdp_1003_hits_srv = pdp_1003_hits.clone();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let listing_body = listing_body.clone();
                let pdp_1001_body = pdp_1001_body.clone();
                let pdp_1002_body = pdp_1002_body.clone();
                let pdp_1003_body = pdp_1003_body.clone();
                let search_hits_srv = search_hits_srv.clone();
                let pdp_1001_hits_srv = pdp_1001_hits_srv.clone();
                let pdp_1002_hits_srv = pdp_1002_hits_srv.clone();
                let pdp_1003_hits_srv = pdp_1003_hits_srv.clone();

                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");

                    let (status, reason, body) = if path.starts_with("/tottus-pe/search") {
                        search_hits_srv.fetch_add(1, Ordering::SeqCst);
                        (200, "OK", listing_body)
                    } else if path.contains("/1001") {
                        pdp_1001_hits_srv.fetch_add(1, Ordering::SeqCst);
                        (200, "OK", pdp_1001_body)
                    } else if path.contains("/1002") {
                        pdp_1002_hits_srv.fetch_add(1, Ordering::SeqCst);
                        (200, "OK", pdp_1002_body)
                    } else if path.contains("/1003") {
                        pdp_1003_hits_srv.fetch_add(1, Ordering::SeqCst);
                        (200, "OK", pdp_1003_body)
                    } else {
                        (404, "Not Found", "missing".to_string())
                    };

                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            true,
            Some(cache_path.clone()),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);

        // First fetch: 1 listing + 3 PDPs (seeded 1001 IS queried because seed is not verified PDP).
        let batch1 = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch1.products.len(), 3);
        assert_eq!(batch1.raw_bytes, expected_batch1_bytes);
        assert_eq!(search_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1001_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1002_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1003_hits.load(Ordering::SeqCst), 1);

        assert_eq!(batch1.products[0].skus[0].ean, None);
        assert_eq!(batch1.products[0].skus[0].ean_source, None);
        assert_eq!(batch1.products[0].skus[0].available_qty, None);
        assert_eq!(
            batch1.products[0].skus[0].availability_status,
            "listed_unknown"
        );
        assert_eq!(
            batch1.products[0].skus[0].availability_source,
            "tottus_listing_presence"
        );

        assert_eq!(batch1.products[1].skus[0].ean, None);
        assert_eq!(batch1.products[1].skus[0].ean_source, None);
        assert_eq!(batch1.products[1].skus[0].available_qty, None);
        assert_eq!(batch1.products[1].skus[0].availability_status, "in_stock");
        assert_eq!(batch1.products[1].skus[0].availability_source, "tottus_pdp");
        assert_eq!(
            batch1.products[1].skus[0].card_program.as_deref(),
            Some("CMR")
        );

        assert_eq!(
            batch1.products[2].skus[0].ean.as_deref(),
            Some("7751271034142")
        );
        assert_eq!(
            batch1.products[2].skus[0].ean_source.as_deref(),
            Some("pdp")
        );
        assert_eq!(batch1.products[2].skus[0].available_qty, Some(0));
        assert_eq!(
            batch1.products[2].skus[0].availability_status,
            "out_of_stock"
        );
        assert_eq!(batch1.products[2].skus[0].availability_source, "tottus_pdp");

        // Verify persisted cache contents: seeded 1001 was overwritten as pdp_checked with ean: None.
        let saved_cache = TottusPdpCache::load(&cache_path).unwrap();
        assert!(saved_cache
            .get("1001")
            .is_some_and(|e| e.pdp_checked && e.source == "tottus_pdp_ssr" && e.ean.is_none()));
        assert!(saved_cache.get("1002").is_some_and(|e| e.pdp_checked
            && e.ean.is_none()
            && e.in_stock == Some(true)
            && e.available_qty.is_none()));
        assert!(saved_cache.get("1003").is_some_and(|e| e.pdp_checked
            && e.source == "tottus_pdp_ssr"
            && e.ean.as_deref() == Some("7751271034142")
            && e.available_qty == Some(0)));

        // Second fetch: ZERO PDP requests, only 1 listing request; cached availability does NOT override listing!
        let batch2 = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch2.raw_bytes, expected_listing_bytes);
        assert_eq!(search_hits.load(Ordering::SeqCst), 2);
        assert_eq!(pdp_1001_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1002_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1003_hits.load(Ordering::SeqCst), 1);

        assert_eq!(batch2.products[0].skus[0].ean, None);
        assert_eq!(batch2.products[0].skus[0].ean_source, None);
        assert_eq!(batch2.products[1].skus[0].available_qty, None);
        assert_eq!(
            batch2.products[1].skus[0].availability_status,
            "listed_unknown"
        );
        assert_eq!(
            batch2.products[1].skus[0].availability_source,
            "tottus_listing_presence"
        );
        assert_eq!(
            batch2.products[2].skus[0].ean.as_deref(),
            Some("7751271034142")
        );
        assert_eq!(
            batch2.products[2].skus[0].ean_source.as_deref(),
            Some("pdp")
        );
        assert_eq!(batch2.products[2].skus[0].available_qty, None);
        assert_eq!(
            batch2.products[2].skus[0].availability_status,
            "listed_unknown"
        );
        assert_eq!(
            batch2.products[2].skus[0].availability_source,
            "tottus_listing_presence"
        );

        // Cache-only mode (enrich_pdp = false) enriches EAN from verified PDP cache with zero PDP hits, without inheriting cached availability.
        let cache_only_adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            false,
            Some(cache_path.clone()),
        );
        let batch3 = cache_only_adapter
            .fetch_page("leche", 0, 20, &ctx)
            .await
            .unwrap();
        assert_eq!(batch3.raw_bytes, expected_listing_bytes);
        assert_eq!(search_hits.load(Ordering::SeqCst), 3);
        assert_eq!(pdp_1001_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1002_hits.load(Ordering::SeqCst), 1);
        assert_eq!(pdp_1003_hits.load(Ordering::SeqCst), 1);
        assert_eq!(batch3.products[0].skus[0].ean, None);
        assert_eq!(batch3.products[0].skus[0].ean_source, None);
        assert_eq!(batch3.products[1].skus[0].available_qty, None);
        assert_eq!(
            batch3.products[1].skus[0].availability_status,
            "listed_unknown"
        );
        assert_eq!(
            batch3.products[2].skus[0].ean.as_deref(),
            Some("7751271034142")
        );
        assert_eq!(
            batch3.products[2].skus[0].ean_source.as_deref(),
            Some("pdp")
        );
        assert_eq!(batch3.products[2].skus[0].available_qty, None);
        assert_eq!(
            batch3.products[2].skus[0].availability_status,
            "listed_unknown"
        );
    }

    #[tokio::test]
    async fn falabella_fetch_page_continues_when_single_pdp_request_fails() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let listing_body = r#"<html><script id="__NEXT_DATA__" type="application/json">{
          "props": {
            "pageProps": {
              "results": [
                {
                  "productId": "P2001",
                  "skuId": "2001",
                  "displayName": "Broken PDP",
                  "url": "/tottus-pe/product/P2001/broken/2001",
                  "offeringId": "off-2001",
                  "prices": [{"type": "internetPrice", "price": ["9.90"]}]
                },
                {
                  "productId": "P2002",
                  "skuId": "2002",
                  "displayName": "Good PDP",
                  "url": "/tottus-pe/product/P2002/good/2002",
                  "offeringId": "off-2002",
                  "prices": [{"type": "internetPrice", "price": ["12.90"]}]
                }
              ]
            }
          }
        }</script></html>"#
            .to_string();

        let good_pdp = r#"<html><script type="application/ld+json">{
          "@type": "Product",
          "gtin13": "7751271034142",
          "offers": {"availability": "https://schema.org/InStock"}
        }</script></html>"#
            .to_string();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let listing_body = listing_body.clone();
                let good_pdp = good_pdp.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let (status, reason, body) = if path.starts_with("/tottus-pe/search") {
                        (200, "OK", listing_body)
                    } else if path.contains("/2002") {
                        (200, "OK", good_pdp)
                    } else {
                        (404, "Not Found", "not found".to_string())
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            true,
            Some(cache_path.clone()),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();

        assert_eq!(batch.products.len(), 2);
        assert_eq!(batch.products[0].skus[0].ean, None);
        assert_eq!(batch.products[0].skus[0].available_qty, None);
        assert_eq!(
            batch.products[0].skus[0].availability_status,
            "listed_unknown"
        );
        assert_eq!(
            batch.products[0].skus[0].availability_source,
            "tottus_listing_presence"
        );
        assert_eq!(
            batch.products[1].skus[0].ean.as_deref(),
            Some("7751271034142")
        );
        assert_eq!(batch.products[1].skus[0].availability_status, "in_stock");
        assert_eq!(batch.products[1].skus[0].availability_source, "tottus_pdp");
        assert_eq!(batch.products[1].skus[0].available_qty, None);

        let saved_cache = TottusPdpCache::load(&cache_path).unwrap();
        assert!(saved_cache.get("2001").is_none());
        assert!(saved_cache.get("2002").is_some_and(|e| e.pdp_checked));
    }

    #[test]
    fn falabella_product_carries_query_term_and_content_hash() {
        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None).with_query_term("leche");
        let body_a = r#"{"props":{"pageProps":{"results":[{"productId":"P1","skuId":"S1","displayName":"Leche","prices":[]}]}}}"#;
        let body_b = r#"{"props":{"pageProps":{"results":[{"displayName":"Leche","prices":[],"skuId":"S1","productId":"P1"}]}}}"#;

        let pa = adapter.parse_page_payload(body_a, &ctx).unwrap();
        let pb = adapter.parse_page_payload(body_b, &ctx).unwrap();
        assert_eq!(pa.len(), 1);
        assert_eq!(pa[0].query_term, "leche");
        assert_eq!(pa[0].content_hash, pb[0].content_hash);
        assert_eq!(pa[0].skus[0].availability_status, "listed_unknown");
        assert_eq!(pa[0].skus[0].availability_source, "tottus_listing_presence");
        assert_eq!(pa[0].skus[0].ean_source, None);
    }

    #[test]
    fn extractor_version_embeds_crate_version_and_git_state() {
        let adapter = FalabellaListingAdapter::new_tottus(test_http());
        let v = adapter.extractor_version();
        assert_eq!(env!("CARGO_PKG_VERSION"), "1.1.0");
        assert!(v.starts_with("falabella-listing-v1.1.0+"));
        let sha = v.rsplit('+').next().unwrap();
        let base = sha.strip_suffix("-dirty").unwrap_or(sha);
        assert!(
            base == "nogit" || (base.len() == 7 && base.chars().all(|c| c.is_ascii_hexdigit()))
        );
    }

    async fn spawn_listing_server(listing_body: String) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let body = listing_body.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = socket.read(&mut buf).await;
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn falabella_seeded_cache_ean_never_reaches_bronze() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut cache_map = std::collections::BTreeMap::new();
        cache_map.insert(
            "S1".to_string(),
            TottusCacheEntry {
                ean: Some("7751271031103".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "vtex_crossref_gtin13_seed".to_string(),
                pdp_checked: false,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&cache_map).unwrap()).unwrap();

        let listing_body = r#"{"props":{"pageProps":{"results":[{"productId":"P1","skuId":"S1","displayName":"Leche Sin EAN","url":"/tottus-pe/product/P1/leche/S1","prices":[]}]}}}"#.to_string();
        let addr = spawn_listing_server(listing_body).await;
        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            false,
            Some(cache_path),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        let sku = &batch.products[0].skus[0];
        assert_eq!(sku.ean, None);
        assert_eq!(sku.ean_source, None);
    }

    #[tokio::test]
    async fn falabella_pdp_cache_ean_sets_source_pdp() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut cache_map = std::collections::BTreeMap::new();
        cache_map.insert(
            "S1".to_string(),
            TottusCacheEntry {
                ean: Some("7751271031103".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "tottus_pdp_ssr".to_string(),
                pdp_checked: true,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&cache_map).unwrap()).unwrap();

        let listing_body = r#"{"props":{"pageProps":{"results":[{"productId":"P1","skuId":"S1","displayName":"Leche Con Cache PDP","url":"/tottus-pe/product/P1/leche/S1","prices":[]}]}}}"#.to_string();
        let addr = spawn_listing_server(listing_body).await;
        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            false,
            Some(cache_path),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        let sku = &batch.products[0].skus[0];
        assert_eq!(sku.ean.as_deref(), Some("7751271031103"));
        assert_eq!(sku.ean_source.as_deref(), Some("pdp"));
    }

    #[test]
    fn record_pdp_enrichment_does_not_launder_seed_ean() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut initial = std::collections::BTreeMap::new();
        initial.insert(
            "S1".to_string(),
            TottusCacheEntry {
                ean: Some("7751271031103".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "vtex_crossref_gtin13_seed".to_string(),
                pdp_checked: false,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&initial).unwrap()).unwrap();

        let mut cache = TottusPdpCache::load(&cache_path).unwrap();
        let enrichment = TottusPdpEnrichment {
            ean: None,
            available_qty: Some(12),
            in_stock: Some(true),
            seller_sku_id: None,
        };
        cache.record_pdp_enrichment("S1", &enrichment);
        let e = cache.get("S1").unwrap();
        assert_eq!(e.ean, None);
        assert_eq!(e.source, "tottus_pdp_ssr");
        assert!(e.pdp_checked);
    }

    #[test]
    fn record_pdp_enrichment_keeps_previous_pdp_ean_when_pdp_returns_null() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut initial = std::collections::BTreeMap::new();
        initial.insert(
            "S1".to_string(),
            TottusCacheEntry {
                ean: Some("7751271031103".to_string()),
                available_qty: None,
                in_stock: None,
                seller_sku_id: None,
                source: "tottus_pdp_ssr".to_string(),
                pdp_checked: true,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&initial).unwrap()).unwrap();

        let mut cache = TottusPdpCache::load(&cache_path).unwrap();
        let enrichment = TottusPdpEnrichment {
            ean: None,
            available_qty: Some(12),
            in_stock: Some(true),
            seller_sku_id: None,
        };
        cache.record_pdp_enrichment("S1", &enrichment);
        assert_eq!(
            cache.get("S1").unwrap().ean.as_deref(),
            Some("7751271031103")
        );
        assert_eq!(cache.get("S1").unwrap().source, "tottus_pdp_ssr");
        assert!(cache.get("S1").unwrap().pdp_checked);
    }

    #[test]
    fn tottus_listing_without_stock_fields_is_listed_unknown_not_qty_1() {
        let item =
            serde_json::json!({"offeringId": "x", "availability": {"homeDeliveryShipping": ""}});
        assert_eq!(
            extract_tottus_listing_availability(&item),
            crate::domain::AvailabilityObs {
                qty: None,
                status: "listed_unknown",
                source: "tottus_listing_presence",
            }
        );
    }

    #[test]
    fn tottus_max_on_cart_amount_is_not_stock() {
        assert_eq!(
            extract_tottus_listing_availability(&serde_json::json!({"maxOnCartAmount": 24})).qty,
            None
        );
    }

    #[test]
    fn tottus_listing_out_of_stock_flag_wins() {
        let item = serde_json::json!({
            "availability": {"isOutOfStock": true},
            "availableQuantity": 5
        });
        assert_eq!(
            extract_tottus_listing_availability(&item),
            crate::domain::AvailabilityObs {
                qty: Some(0),
                status: "out_of_stock",
                source: "tottus_listing_flag",
            }
        );
    }

    #[tokio::test]
    async fn tottus_cached_availability_never_overrides_listing() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let mut cache_map = std::collections::BTreeMap::new();
        cache_map.insert(
            "S100".to_string(),
            TottusCacheEntry {
                ean: Some("7751271031103".to_string()),
                available_qty: Some(0),
                in_stock: Some(false),
                seller_sku_id: None,
                source: TOTTUS_CACHE_SOURCE_PDP.to_string(),
                pdp_checked: true,
                updated_at: chrono::Utc::now(),
            },
        );
        std::fs::write(&cache_path, serde_json::to_vec_pretty(&cache_map).unwrap()).unwrap();

        let listing_body = r#"{"props":{"pageProps":{"results":[
          {
            "productId": "P100",
            "skuId": "S100",
            "displayName": "Leche Sin Stock En Listado",
            "url": "/tottus-pe/product/P100/leche/S100",
            "offeringId": "off-100",
            "prices": [{"type": "internetPrice", "price": ["5.90"]}]
          }
        ]}}}"#
            .to_string();
        let addr = spawn_listing_server(listing_body).await;
        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            false,
            Some(cache_path),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();
        let sku = &batch.products[0].skus[0];
        assert_eq!(sku.ean.as_deref(), Some("7751271031103"));
        assert_eq!(sku.ean_source.as_deref(), Some("pdp"));
        assert_eq!(sku.available_qty, None);
        assert_eq!(sku.availability_status, "listed_unknown");
        assert_eq!(sku.availability_source, "tottus_listing_presence");
    }

    #[tokio::test]
    async fn tottus_pdp_fetched_this_run_sets_pdp_source() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache_path = dir.path().join("tottus_ean_cache.json");

        let listing_body = r#"{"props":{"pageProps":{"results":[
          {
            "productId": "P-OOS",
            "skuId": "S-OOS",
            "displayName": "Leche Agotada PDP",
            "url": "/tottus-pe/product/P-OOS/leche-oos/S-OOS",
            "offeringId": "off-oos",
            "prices": [{"type": "internetPrice", "price": ["5.90"]}]
          },
          {
            "productId": "P-IN",
            "skuId": "S-IN",
            "displayName": "Leche En Stock PDP",
            "url": "/tottus-pe/product/P-IN/leche-in/S-IN",
            "offeringId": "off-in",
            "prices": [{"type": "internetPrice", "price": ["6.50"]}]
          }
        ]}}}"#
            .to_string();

        let pdp_oos = r#"<html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "offers": {"@type": "Offer", "availability": "https://schema.org/OutOfStock"}
        }</script></head></html>"#
            .to_string();

        let pdp_in = r#"<html><head>
        <script type="application/ld+json">{
          "@type": "Product",
          "offers": {"@type": "Offer", "availability": "https://schema.org/InStock"}
        }</script></head><body>
        <script id="__NEXT_DATA__" type="application/json">{
          "props": {"pageProps": {"productData": {
            "isOutOfStock": false,
            "stockUnits": {"S-IN": 7},
            "variants": [{"id": "S-IN"}]
          }}}
        }</script></body></html>"#
            .to_string();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let listing_body = listing_body.clone();
                let pdp_oos = pdp_oos.clone();
                let pdp_in = pdp_in.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let n = socket.read(&mut buf).await.unwrap_or(0);
                    let req = String::from_utf8_lossy(&buf[..n]);
                    let path = req
                        .lines()
                        .next()
                        .and_then(|line| line.split_whitespace().nth(1))
                        .unwrap_or("/");
                    let (status, reason, body) = if path.starts_with("/tottus-pe/search") {
                        (200, "OK", listing_body)
                    } else if path.contains("/S-OOS") {
                        (200, "OK", pdp_oos)
                    } else if path.contains("/S-IN") {
                        (200, "OK", pdp_in)
                    } else {
                        (404, "Not Found", "missing".to_string())
                    };
                    let resp = format!(
                        "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                });
            }
        });

        let adapter = FalabellaListingAdapter::with_enrichment(
            format!("http://{addr}"),
            test_http(),
            true,
            Some(cache_path),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("leche", 0, 20, &ctx).await.unwrap();

        let oos_sku = &batch.products[0].skus[0];
        assert_eq!(oos_sku.availability_status, "out_of_stock");
        assert_eq!(oos_sku.availability_source, "tottus_pdp");
        assert_eq!(oos_sku.available_qty, Some(0));

        let in_sku = &batch.products[1].skus[0];
        assert_eq!(in_sku.availability_status, "in_stock");
        assert_eq!(in_sku.availability_source, "tottus_pdp");
        assert_eq!(in_sku.available_qty, Some(7));
    }
}
