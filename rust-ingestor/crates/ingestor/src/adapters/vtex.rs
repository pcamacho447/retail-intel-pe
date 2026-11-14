use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde_json::Value;

use crate::adapter::{FetchContext, PageBatch, RetailerAdapter};
use crate::adapters::{absolutize_url, encode_url_segment, value_as_string};
use crate::dedup::{compute_content_hash, compute_payload_hash};
use crate::domain::{normalize_opt_string, validate_gtin_mod10, Product, SkuItem};
use crate::http::RateLimitedHttpClient;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VtexRetailer {
    PlazaVea,
    Metro,
    Wong,
}

impl VtexRetailer {
    pub fn slug(&self) -> &'static str {
        match self {
            Self::PlazaVea => "plazavea",
            Self::Metro => "metro",
            Self::Wong => "wong",
        }
    }

    pub fn base_url(&self) -> &'static str {
        match self {
            Self::PlazaVea => "https://www.plazavea.com.pe",
            Self::Metro => "https://www.metro.pe",
            Self::Wong => "https://www.wong.pe",
        }
    }
}

pub struct VtexAdapter {
    retailer: VtexRetailer,
    base_url: String,
    http: Arc<RateLimitedHttpClient>,
}

impl VtexAdapter {
    pub fn new(retailer: VtexRetailer, http: Arc<RateLimitedHttpClient>) -> Self {
        Self::with_base_url(retailer, retailer.base_url(), http)
    }

    pub fn with_base_url(
        retailer: VtexRetailer,
        base_url: impl Into<String>,
        http: Arc<RateLimitedHttpClient>,
    ) -> Self {
        Self {
            retailer,
            base_url: base_url.into().trim_end_matches('/').to_string(),
            http,
        }
    }
}

fn positive_f64(val: Option<&Value>) -> Option<f64> {
    let num = val?.as_f64()?;
    (num.is_finite() && num > 0.0).then_some(num)
}

fn parse_positive_param_f64(val: Option<&Value>) -> Option<f64> {
    let v = val?;
    if let Some(num) = positive_f64(Some(v)) {
        return Some(num);
    }
    let num = v.as_str()?.trim().parse::<f64>().ok()?;
    (num.is_finite() && num > 0.0).then_some(num)
}

/// Looks up a VTEX JSON field across PascalCase, camelCase, and C# `<Field>k__BackingField`
/// auto-property keys frequently serialized inside `commertialOffer.Teasers`.
fn vtex_field<'a>(obj: &'a Value, pascal: &str) -> Option<&'a Value> {
    if let Some(val) = obj.get(pascal).filter(|v| !v.is_null()) {
        return Some(val);
    }
    let mut chars = pascal.chars();
    if let Some(first) = chars.next() {
        let camel = format!("{}{}", first.to_ascii_lowercase(), chars.as_str());
        if let Some(val) = obj.get(&camel).filter(|v| !v.is_null()) {
            return Some(val);
        }
    }
    let backing = format!("<{pascal}>k__BackingField");
    obj.get(&backing).filter(|v| !v.is_null())
}

fn vtex_parameters(parent: Option<&Value>) -> &[Value] {
    parent
        .and_then(|p| vtex_field(p, "Parameters"))
        .and_then(|v| v.as_array())
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn vtex_param_kv(param: &Value) -> Option<(String, Option<&Value>)> {
    let name = vtex_field(param, "Name")
        .and_then(|v| v.as_str())
        .and_then(|s| normalize_opt_string(Some(s)))?;
    Some((name, vtex_field(param, "Value")))
}

fn split_param_tokens(val: &str) -> impl Iterator<Item = &str> {
    val.split(|c: char| c == ',' || c == ';' || c.is_whitespace())
        .map(str::trim)
        .filter(|s| !s.is_empty())
}

fn validate_value_gtin(val: Option<&Value>) -> Option<String> {
    value_as_string(val).and_then(|s| validate_gtin_mod10(&s))
}

fn extract_vtex_ean(item: &Value, product_elem: &Value) -> Option<String> {
    if let Some(ean) = normalize_opt_string(item.get("ean").and_then(|v| v.as_str())) {
        return Some(ean);
    }

    // VTEX Plaza Vea often leaves items[].ean empty while storing a valid EAN-8 in
    // referenceId[].Value alongside invalid internal IDs (e.g. 926320, 20181521)
    // that GS1 Mod-10 rejects.
    if let Some(refs) = item.get("referenceId").and_then(|v| v.as_array()) {
        for entry in refs {
            if let Some(gtin) = validate_value_gtin(vtex_field(entry, "Value")) {
                return Some(gtin);
            }
        }
    }

    for key in ["productReference", "productReferenceCode"] {
        if let Some(gtin) = validate_value_gtin(product_elem.get(key)) {
            return Some(gtin);
        }
    }

    const SPEC_EAN_KEYS: [&str; 8] = [
        "EAN",
        "ean",
        "Código EAN",
        "Codigo EAN",
        "Código de barras",
        "Codigo de barras",
        "GTIN",
        "gtin",
    ];
    for key in SPEC_EAN_KEYS {
        let Some(val) = product_elem.get(key) else {
            continue;
        };
        let candidates = val
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(std::slice::from_ref(val));
        for elem in candidates {
            if let Some(gtin) = validate_value_gtin(Some(elem)) {
                return Some(gtin);
            }
        }
    }

    None
}

fn teaser_targets_sku(params: &[Value], sku_id: &str) -> bool {
    let target_ids = params.iter().find_map(|param| {
        let (name, val) = vtex_param_kv(param)?;
        if name.eq_ignore_ascii_case("PromotionalPriceTableItemsIds") {
            value_as_string(val)
        } else {
            None
        }
    });
    target_ids.is_none_or(|ids_val| split_param_tokens(&ids_val).any(|id| id == sku_id))
}

fn compute_param_card_price(
    param_name: &str,
    val: f64,
    list_price: Option<f64>,
    selling_price: Option<f64>,
) -> Option<f64> {
    let name = param_name.to_ascii_lowercase();
    if name.contains("shipping") {
        return None;
    }

    let candidate = if name == "promotionalpricetableitemsdiscount" {
        // VTEX price-table discounts subtract from ListPrice rather than Price.
        list_price
            .or(selling_price)
            .filter(|base| *base > val)
            .map(|base| ((base - val) * 100.0).round() / 100.0)
            .filter(|p| selling_price.is_none_or(|sp| *p < sp))
    } else if name == "maximumunitpricediscount" {
        // VTEX encodes MaximumUnitPriceDiscount in integer centavos (e.g. "41900" -> 419.00).
        let price = val.round() / 100.0;
        Some(price).filter(|p| selling_price.is_none_or(|sp| *p < sp))
    } else if name.contains("percentualdiscount") {
        selling_price
            .filter(|_| val < 100.0)
            .map(|sp| ((sp * (1.0 - val / 100.0)) * 100.0).round() / 100.0)
    } else if name.contains("nominaldiscount") {
        selling_price
            .filter(|sp| *sp > val)
            .map(|sp| ((sp - val) * 100.0).round() / 100.0)
    } else if name == "promotionalprice" || name == "cardprice" {
        Some(val)
    } else {
        None
    };

    candidate.filter(|p| p.is_finite() && *p > 0.0)
}

fn classify_from_condition_params(
    params: &[Value],
    retailer: VtexRetailer,
) -> Option<&'static str> {
    const CENCOSUD_BINS: [&str; 4] = ["905051", "477357", "489068", "512533"];
    const OH_PAYMENT_METHODS: [&str; 3] = ["210", "208", "202"];

    let mut has_oh_payment_method = false;
    for param in params {
        let Some((name, val)) = vtex_param_kv(param) else {
            continue;
        };
        let Some(raw) = value_as_string(val) else {
            continue;
        };
        if name.eq_ignore_ascii_case("RestrictionsBins")
            && split_param_tokens(&raw)
                .any(|tok| CENCOSUD_BINS.iter().any(|bin| tok.starts_with(bin)))
        {
            return Some("Cencosud");
        }
        if retailer == VtexRetailer::PlazaVea
            && name.eq_ignore_ascii_case("PaymentMethodId")
            && split_param_tokens(&raw).any(|tok| OH_PAYMENT_METHODS.contains(&tok))
        {
            has_oh_payment_method = true;
        }
    }

    has_oh_payment_method.then_some("Oh!")
}

fn classify_card_program(teaser: &Value, retailer: VtexRetailer) -> String {
    let teaser_name = vtex_field(teaser, "Name")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_lowercase();

    if teaser_name.contains("prime") {
        return "Wong Prime".to_string();
    }
    if teaser_name.contains("oh") {
        return "Oh!".to_string();
    }
    if ["tcenco", "cencosud", "tc metro", "tc wong"]
        .iter()
        .any(|kw| teaser_name.contains(kw))
    {
        return "Cencosud".to_string();
    }

    let cond_params = vtex_parameters(vtex_field(teaser, "Conditions"));
    if let Some(program) = classify_from_condition_params(cond_params, retailer) {
        return program.to_string();
    }

    match retailer {
        VtexRetailer::PlazaVea => "Oh!".to_string(),
        VtexRetailer::Metro | VtexRetailer::Wong => "Cencosud".to_string(),
    }
}

fn extract_card_offer(
    offer: &Value,
    sku_id: &str,
    list_price: Option<f64>,
    selling_price: Option<f64>,
    retailer: VtexRetailer,
) -> (Option<f64>, Option<String>) {
    let mut best: Option<(f64, String)> = None;

    for key in ["PromotionTeasers", "Teasers"] {
        let Some(teasers) = vtex_field(offer, key).and_then(|v| v.as_array()) else {
            continue;
        };
        for teaser in teasers {
            let conditions = vtex_field(teaser, "Conditions");
            let min_qty = conditions
                .and_then(|c| vtex_field(c, "MinimumQuantity"))
                .and_then(|v| {
                    v.as_u64()
                        .or_else(|| v.as_str()?.trim().parse::<u64>().ok())
                })
                .unwrap_or(0);
            if min_qty > 1 {
                continue;
            }

            let params = vtex_parameters(vtex_field(teaser, "Effects"));
            if params.is_empty() || !teaser_targets_sku(params, sku_id) {
                continue;
            }

            let teaser_best = params
                .iter()
                .filter_map(|param| {
                    let (name, val) = vtex_param_kv(param)?;
                    let num = parse_positive_param_f64(val)?;
                    compute_param_card_price(&name, num, list_price, selling_price)
                })
                .reduce(f64::min);

            if let Some(price) = teaser_best {
                if best.as_ref().is_none_or(|(curr, _)| price < *curr) {
                    best = Some((price, classify_card_program(teaser, retailer)));
                }
            }
        }
    }

    match best {
        Some((price, program)) => (Some(price), Some(program)),
        None => (None, None),
    }
}

fn parse_vtex_sku(
    item: &Value,
    product_elem: &Value,
    fallback_name: &str,
    retailer: VtexRetailer,
) -> Option<SkuItem> {
    let sku_id = value_as_string(item.get("itemId"))?;
    let ean = extract_vtex_ean(item, product_elem);
    let raw_name = normalize_opt_string(item.get("nameComplete").and_then(|v| v.as_str()))
        .or_else(|| normalize_opt_string(item.get("name").and_then(|v| v.as_str())))
        .unwrap_or_else(|| fallback_name.to_string());

    let image_url = item
        .get("images")
        .and_then(|v| v.as_array())
        .and_then(|imgs| imgs.first())
        .and_then(|img| img.get("imageUrl"))
        .and_then(|v| v.as_str())
        .and_then(|s| normalize_opt_string(Some(s)));

    let sellers = item.get("sellers").and_then(|v| v.as_array());
    let selected_seller = sellers.and_then(|arr| {
        arr.iter()
            .find(|s| s.get("sellerDefault").and_then(|v| v.as_bool()) == Some(true))
            .or_else(|| arr.first())
    });

    let (seller_id, list_price, selling_price, card_price, card_program, available_qty) =
        if let Some(seller) = selected_seller {
            let seller_id = value_as_string(seller.get("sellerId"));
            let offer = seller.get("commertialOffer").unwrap_or(&Value::Null);
            let list_price = positive_f64(offer.get("ListPrice"));
            let selling_price = positive_f64(offer.get("Price"));
            let (card_price, card_program) =
                extract_card_offer(offer, &sku_id, list_price, selling_price, retailer);
            let available_qty = offer
                .get("AvailableQuantity")
                .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
            (
                seller_id,
                list_price,
                selling_price,
                card_price,
                card_program,
                available_qty,
            )
        } else {
            (None, None, None, None, None, None)
        };

    Some(SkuItem {
        sku_id,
        ean,
        ean_source: None,
        availability_status: "unknown".to_string(),
        availability_source: "none".to_string(),
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
        available_qty,
    })
}

#[async_trait]
impl RetailerAdapter for VtexAdapter {
    fn retailer_slug(&self) -> &'static str {
        self.retailer.slug()
    }

    fn extractor_version(&self) -> &'static str {
        "vtex-catalog-v1.0.0"
    }

    fn volatile_payload_keys(&self) -> &'static [&'static str] {
        &[
            "PriceToken",
            "PriceValidUntil",
            "CacheVersionUsedToCallCheckout",
        ]
    }

    fn build_page_url(&self, category_or_query: &str, offset: usize, page_size: usize) -> String {
        let effective_size = page_size.clamp(1, 50);
        let to = offset + effective_size - 1;
        let trimmed = category_or_query.trim().trim_matches('/');
        let encoded_path = trimmed
            .split('/')
            .filter(|seg| !seg.is_empty())
            .map(encode_url_segment)
            .collect::<Vec<_>>()
            .join("/");

        format!(
            "{}/api/catalog_system/pub/products/search/{}?_from={}&_to={}",
            self.base_url, encoded_path, offset, to
        )
    }

    fn parse_page_payload(&self, body: &str, ctx: &FetchContext) -> anyhow::Result<Vec<Product>> {
        let parsed: Value = serde_json::from_str(body).with_context(|| {
            let snippet: String = body.chars().take(200).collect();
            format!(
                "VTEX payload for {} is not valid JSON (http_status={}): snippet={snippet:?}",
                self.retailer_slug(),
                ctx.http_status
            )
        })?;

        let items = parsed.as_array().ok_or_else(|| {
            let snippet: String = body.chars().take(200).collect();
            anyhow!(
                "VTEX payload for {} expected JSON array (http_status={}): snippet={snippet:?}",
                self.retailer_slug(),
                ctx.http_status
            )
        })?;

        let mut products = Vec::with_capacity(items.len());
        let volatile_keys = self.volatile_payload_keys();
        for elem in items {
            let raw_payload = serde_json::to_string(elem)?;
            let payload_hash = compute_payload_hash(&raw_payload);
            let content_hash = compute_content_hash(&raw_payload, volatile_keys);
            let product_id = value_as_string(elem.get("productId")).ok_or_else(|| {
                anyhow!(
                    "VTEX product object missing productId (retailer={})",
                    self.retailer_slug()
                )
            })?;
            let raw_name = normalize_opt_string(
                elem.get("productName")
                    .or_else(|| elem.get("productTitle"))
                    .and_then(|v| v.as_str()),
            )
            .unwrap_or_else(|| product_id.clone());

            let brand = normalize_opt_string(elem.get("brand").and_then(|v| v.as_str()));
            let category_path = elem
                .get("categories")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|v| v.as_str())
                .and_then(|s| normalize_opt_string(Some(s)));
            let raw_description =
                normalize_opt_string(elem.get("description").and_then(|v| v.as_str()));
            let product_url =
                absolutize_url(&self.base_url, elem.get("link").and_then(|v| v.as_str()));

            let skus = elem
                .get("items")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|sku| parse_vtex_sku(sku, elem, &raw_name, self.retailer))
                        .collect()
                })
                .unwrap_or_default();

            products.push(Product {
                retailer: self.retailer_slug().to_string(),
                market_id: ctx.market_id.clone(),
                store_id: ctx.store_id.clone(),
                product_id,
                raw_name,
                brand,
                category_path,
                raw_description,
                product_url,
                currency: ctx.currency.clone(),
                sales_channel: ctx.sales_channel.clone(),
                postal_code: ctx.postal_code.clone(),
                scraped_at: ctx.scraped_at,
                extraction_run_id: ctx.extraction_run_id,
                extractor_version: self.extractor_version().to_string(),
                source_endpoint: ctx.source_endpoint.clone(),
                http_status: ctx.http_status,
                payload_hash,
                raw_payload,
                query_term: ctx.query_term.clone(),
                content_hash,
                skus,
            });
        }

        Ok(products)
    }

    async fn fetch_page(
        &self,
        category_or_query: &str,
        offset: usize,
        page_size: usize,
        ctx: &FetchContext,
    ) -> anyhow::Result<PageBatch> {
        let effective_size = page_size.clamp(1, 50);
        let url = self.build_page_url(category_or_query, offset, effective_size);
        let resp = self.http.get_text(&url).await?;
        let page_ctx = ctx.clone().with_endpoint(url, resp.status);
        let products = self.parse_page_payload(&resp.body, &page_ctx)?;
        let count = products.len();
        let has_more = count > 0 && count >= effective_size;

        Ok(PageBatch {
            products,
            has_more,
            next_offset: offset + count,
            raw_bytes: resp.bytes,
            http_status: resp.status,
            latency_ms: resp.latency_ms,
            products_rejected: 0,
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
    fn vtex_build_page_url_formats_from_to_for_all_retailers() {
        let http = test_http();

        let pv = VtexAdapter::new(VtexRetailer::PlazaVea, http.clone());
        assert_eq!(pv.retailer_slug(), "plazavea");
        assert_eq!(
            pv.build_page_url("lacteos", 0, 20),
            "https://www.plazavea.com.pe/api/catalog_system/pub/products/search/lacteos?_from=0&_to=19"
        );

        let metro = VtexAdapter::new(VtexRetailer::Metro, http.clone());
        assert_eq!(metro.retailer_slug(), "metro");
        assert_eq!(
            metro.build_page_url("/abarrotes", 20, 20),
            "https://www.metro.pe/api/catalog_system/pub/products/search/abarrotes?_from=20&_to=39"
        );

        let wong = VtexAdapter::new(VtexRetailer::Wong, http);
        assert_eq!(wong.retailer_slug(), "wong");
        assert_eq!(
            wong.build_page_url("bebidas", 40, 10),
            "https://www.wong.pe/api/catalog_system/pub/products/search/bebidas?_from=40&_to=49"
        );
    }

    #[test]
    fn vtex_parses_multi_sku_product_normalizes_empty_ean_and_extracts_teasers() {
        let fixture = r#"[
          {
            "productId": "20402646",
            "productName": "Leche Reconstituida Entera Gloria",
            "brand": "Gloria",
            "categories": ["/Lácteos y Huevos/Leches/Leche Evaporada/"],
            "description": "Leche evaporada Gloria lata 390g",
            "link": "/leche-reconstituida-entera-gloria-lata-390g-20402646/p",
            "items": [
              {
                "itemId": "1001",
                "ean": "7750243012345",
                "nameComplete": "Leche Gloria Entera Lata 390g Unidad",
                "images": [{"imageUrl": "https://plazavea.vteximg.com.br/arquivos/ids/1001.jpg"}],
                "sellers": [
                  {
                    "sellerId": "1",
                    "sellerDefault": true,
                    "commertialOffer": {
                      "ListPrice": 5.00,
                      "Price": 4.20,
                      "AvailableQuantity": 35,
                      "PromotionTeasers": [
                        {
                          "Name": "Tarjeta Oh! 10%",
                          "Effects": {
                            "Parameters": [
                              {"Name": "PercentualDiscount", "Value": "10.0"}
                            ]
                          }
                        }
                      ]
                    }
                  }
                ]
              },
              {
                "itemId": "1002",
                "ean": "",
                "name": "Leche Gloria Entera 6 Pack",
                "images": [],
                "sellers": [
                  {
                    "sellerId": "1",
                    "commertialOffer": {
                      "ListPrice": 28.00,
                      "Price": 24.90,
                      "AvailableQuantity": 5,
                      "Teasers": []
                    }
                  }
                ]
              }
            ]
          },
          {
            "productId": "999999",
            "productName": "Producto Sin Oferta",
            "brand": "Generica",
            "categories": ["/Abarrotes/"],
            "description": "",
            "link": "https://www.plazavea.com.pe/sin-oferta/p",
            "items": [
              {
                "itemId": "9999991",
                "ean": "   ",
                "nameComplete": "Producto Sin Oferta",
                "images": [],
                "sellers": []
              }
            ]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", Some("150101".to_string()))
            .with_endpoint(
                "https://www.plazavea.com.pe/api/catalog_system/pub/products/search/lacteos?_from=0&_to=19",
                206,
            );

        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(products.len(), 2);

        let p1 = &products[0];
        assert_eq!(p1.retailer, "plazavea");
        assert_eq!(p1.product_id, "20402646");
        assert_eq!(p1.brand.as_deref(), Some("Gloria"));
        assert_eq!(
            p1.category_path.as_deref(),
            Some("/Lácteos y Huevos/Leches/Leche Evaporada/")
        );
        assert_eq!(
            p1.product_url.as_deref(),
            Some("https://www.plazavea.com.pe/leche-reconstituida-entera-gloria-lata-390g-20402646/p")
        );
        assert_eq!(p1.payload_hash.len(), 64);
        assert_eq!(p1.skus.len(), 2);

        assert_eq!(p1.skus[0].sku_id, "1001");
        assert_eq!(p1.skus[0].ean.as_deref(), Some("7750243012345"));
        assert_eq!(p1.skus[0].list_price, Some(5.00));
        assert_eq!(p1.skus[0].selling_price, Some(4.20));
        assert_eq!(p1.skus[0].card_price, Some(3.78));
        assert_eq!(p1.skus[0].card_program.as_deref(), Some("Oh!"));
        assert_eq!(p1.skus[0].available_qty, Some(35));

        assert_eq!(p1.skus[1].sku_id, "1002");
        assert_eq!(p1.skus[1].ean, None);
        assert_eq!(p1.skus[1].card_price, None);
        assert_eq!(p1.skus[1].card_program, None);
        assert_eq!(p1.skus[1].selling_price, Some(24.90));

        let p2 = &products[1];
        assert_eq!(p2.raw_description, None);
        assert_eq!(p2.skus[0].ean, None);
        assert_eq!(p2.skus[0].seller_id, None);
        assert_eq!(p2.skus[0].list_price, None);
        assert_eq!(p2.skus[0].selling_price, None);
        assert_eq!(p2.skus[0].card_program, None);
        assert_eq!(p2.skus[0].available_qty, None);
    }

    #[test]
    fn vtex_parses_empty_array_without_error() {
        let adapter = VtexAdapter::new(VtexRetailer::Metro, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload("[]", &ctx).unwrap();
        assert!(products.is_empty());
    }

    #[test]
    fn vtex_extracts_card_price_from_price_table_discount_and_max_unit_price_ignoring_shipping() {
        let fixture = r#"[
          {
            "productId": "100470717",
            "productName": "Filete de Atun CAMPOMAR 150g",
            "items": [{
              "itemId": "10672464",
              "name": "Filete de Atun",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 5.90,
                  "Price": 5.20,
                  "AvailableQuantity": 10,
                  "PromotionTeasers": [{
                    "Name": "TARJETA OH - PVEA",
                    "Effects": {
                      "Parameters": [
                        {"Name": "PromotionalPriceTableItemsIds", "Value": "10672464"},
                        {"Name": "PromotionalPriceTableItemsDiscount", "Value": "1.00"}
                      ]
                    }
                  }]
                }
              }]
            }]
          },
          {
            "productId": "999001",
            "productName": "Televisor Samsung 50",
            "items": [{
              "itemId": "9990011",
              "name": "Televisor Samsung 50",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 1499.00,
                  "Price": 1299.00,
                  "AvailableQuantity": 5,
                  "PromotionTeasers": [
                    {
                      "Name": "Metro Delivery Gratis",
                      "Effects": {
                        "Parameters": [{"Name": "ShippingPercentualDiscount", "Value": "1"}]
                      }
                    },
                    {
                      "Name": "Con TC Metro",
                      "Effects": {
                        "Parameters": [{"Name": "MaximumUnitPriceDiscount", "Value": "119900"}]
                      }
                    }
                  ]
                }
              }]
            }]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(products[0].skus[0].card_price, Some(4.90));
        assert_eq!(products[0].skus[0].card_program.as_deref(), Some("Oh!"));
        assert_eq!(products[1].skus[0].card_price, Some(1199.00));
        assert_eq!(
            products[1].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );
    }

    #[test]
    fn vtex_ignores_teaser_when_sku_id_not_targeted_or_min_qty_gt_1() {
        let fixture = r#"[
          {
            "productId": "500",
            "productName": "Pack Promo",
            "items": [{
              "itemId": "5001",
              "name": "Pack Promo",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 10.00,
                  "Price": 9.00,
                  "AvailableQuantity": 10,
                  "PromotionTeasers": [
                    {
                      "Name": "Lleva 2",
                      "Conditions": {"MinimumQuantity": 2, "Parameters": []},
                      "Effects": {
                        "Parameters": [{"Name": "PercentualDiscount", "Value": "20.0"}]
                      }
                    },
                    {
                      "Name": "Other SKU",
                      "Conditions": {"MinimumQuantity": 0, "Parameters": []},
                      "Effects": {
                        "Parameters": [
                          {"Name": "PromotionalPriceTableItemsIds", "Value": "9999"},
                          {"Name": "PromotionalPriceTableItemsDiscount", "Value": "2.00"}
                        ]
                      }
                    }
                  ]
                }
              }]
            }]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(products[0].skus[0].card_price, None);
        assert_eq!(products[0].skus[0].card_program, None);
    }

    #[tokio::test]
    async fn vtex_clamps_page_size_to_50_to_match_api_limit() {
        let items: Vec<String> = (1..=50)
            .map(|i| format!(r#"{{"productId":"{i}","productName":"P{i}","items":[{{"itemId":"s{i}","name":"P{i}","sellers":[]}}]}}"#))
            .collect();
        let body = format!("[{}]", items.join(","));

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });

        let adapter = VtexAdapter::with_base_url(
            VtexRetailer::PlazaVea,
            format!("http://{addr}"),
            test_http(),
        );
        assert_eq!(
            adapter.build_page_url("lacteos", 0, 100),
            format!("http://{addr}/api/catalog_system/pub/products/search/lacteos?_from=0&_to=49")
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("lacteos", 0, 100, &ctx).await.unwrap();
        assert_eq!(batch.products.len(), 50);
        assert!(batch.has_more);
        assert_eq!(batch.next_offset, 50);
    }

    #[tokio::test]
    async fn vtex_fetch_page_stops_when_fewer_results_than_page_size() {
        let body = r#"[{"productId":"1","productName":"Item 1","items":[{"itemId":"11","name":"Item 1","sellers":[]}]}]"#;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = socket.read(&mut buf).await;
            let resp = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        });

        let adapter = VtexAdapter::with_base_url(
            VtexRetailer::PlazaVea,
            format!("http://{addr}"),
            test_http(),
        );
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("lacteos", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch.http_status, 206);
        assert_eq!(batch.products.len(), 1);
        assert!(!batch.has_more);
        assert_eq!(batch.next_offset, 1);
    }

    #[test]
    fn vtex_ean_fallback_extracts_valid_mod10_and_rejects_invalid_candidates() {
        let fixture = r#"[
          {
            "productId": "p-ref-valid",
            "productName": "Aceite Primor",
            "productReference": "926320",
            "Código EAN": ["7751271034142"],
            "items": [
              {
                "itemId": "sku-ref-1",
                "ean": "",
                "name": "Aceite Primor 1L",
                "referenceId": [
                  {"Key": "RefId", "Value": "926320"},
                  {"Key": "RefId2", "Value": "20181521"},
                  {"Key": "RefId3", "Value": "VTCPERU-123"},
                  {"Key": "EAN", "Value": "20181512"}
                ],
                "sellers": []
              },
              {
                "itemId": "sku-ref-2",
                "ean": null,
                "name": "Aceite Primor 500ml",
                "referenceId": [
                  {"Key": "RefId", "value": 20570316}
                ],
                "sellers": []
              }
            ]
          },
          {
            "productId": "p-spec-fallback",
            "productName": "Arroz Costeño",
            "productReference": "VTCPERU-123",
            "productReferenceCode": "20181521",
            "Código EAN": ["invalid-ean", "7751271034142"],
            "items": [
              {
                "itemId": "sku-spec-1",
                "ean": "   ",
                "name": "Arroz Costeño 5kg",
                "referenceId": [{"Key": "RefId", "Value": "926320"}],
                "sellers": []
              }
            ]
          },
          {
            "productId": "p-prodref-fallback",
            "productName": "Azucar Rubia",
            "productReference": "012345678905",
            "productReferenceCode": "17750151008669",
            "items": [
              {
                "itemId": "sku-prodref-1",
                "ean": "",
                "name": "Azucar Rubia 1kg",
                "sellers": []
              }
            ]
          },
          {
            "productId": "p-prodrefcode-and-str-spec",
            "productName": "Fideos Don Vittorio",
            "productReference": "926320",
            "productReferenceCode": "17750151008669",
            "items": [
              {
                "itemId": "sku-prodrefcode-1",
                "ean": "",
                "name": "Fideos 500g",
                "sellers": []
              }
            ]
          },
          {
            "productId": "p-gtin-str-spec",
            "productName": "Galleta Soda",
            "GTIN": "7751271034142",
            "items": [
              {
                "itemId": "sku-gtin-1",
                "ean": "",
                "name": "Galleta Soda 6pk",
                "sellers": []
              }
            ]
          },
          {
            "productId": "p-all-invalid",
            "productName": "Producto Interno Sin GTIN",
            "productReference": "926320",
            "productReferenceCode": "20181521",
            "EAN": ["VTCPERU-123", "00000000"],
            "items": [
              {
                "itemId": "sku-none-1",
                "ean": "",
                "name": "Sin GTIN",
                "referenceId": [
                  {"Key": "RefId", "Value": "926320"},
                  {"Key": "RefId2", "Value": "20181521"},
                  {"Key": "RefId3", "Value": "VTCPERU-123"}
                ],
                "sellers": []
              }
            ]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();

        assert_eq!(products[0].skus[0].ean.as_deref(), Some("20181512"));
        assert_eq!(products[0].skus[1].ean.as_deref(), Some("20570316"));
        assert_eq!(products[1].skus[0].ean.as_deref(), Some("7751271034142"));
        assert_eq!(products[2].skus[0].ean.as_deref(), Some("012345678905"));
        assert_eq!(products[3].skus[0].ean.as_deref(), Some("17750151008669"));
        assert_eq!(products[4].skus[0].ean.as_deref(), Some("7751271034142"));
        assert_eq!(products[5].skus[0].ean, None);
    }

    #[test]
    fn vtex_plazavea_tarjeta_oh_sets_card_price_and_oh_program() {
        let fixture = r#"[
          {
            "productId": "100470717",
            "productName": "Filete de Atun CAMPOMAR 150g",
            "items": [
              {
                "itemId": "10672464",
                "name": "Filete de Atun",
                "sellers": [{
                  "sellerId": "1",
                  "commertialOffer": {
                    "ListPrice": 5.90,
                    "Price": 5.20,
                    "AvailableQuantity": 10,
                    "PromotionTeasers": [{
                      "Name": "TARJETA OH - PVEA",
                      "Conditions": {
                        "MinimumQuantity": 0,
                        "Parameters": [{"Name": "PaymentMethodId", "Value": "210"}]
                      },
                      "Effects": {
                        "Parameters": [
                          {"Name": "PromotionalPriceTableItemsIds", "Value": "10672464"},
                          {"Name": "PromotionalPriceTableItemsDiscount", "Value": "1.00"}
                        ]
                      }
                    }]
                  }
                }]
              },
              {
                "itemId": "10672465",
                "name": "Filete sin promo tarjeta",
                "sellers": [{
                  "sellerId": "1",
                  "commertialOffer": {
                    "ListPrice": 5.90,
                    "Price": 5.20,
                    "AvailableQuantity": 10,
                    "PromotionTeasers": []
                  }
                }]
              }
            ]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();

        assert_eq!(products[0].skus[0].card_price, Some(4.90));
        assert_eq!(products[0].skus[0].card_program.as_deref(), Some("Oh!"));
        assert_eq!(products[0].skus[1].card_price, None);
        assert_eq!(products[0].skus[1].card_program, None);
    }

    #[test]
    fn vtex_parses_csharp_backing_fields_in_teasers_when_promotion_teasers_empty() {
        let fixture = r#"[
          {
            "productId": "3001",
            "productName": "Cafe Instantaneo",
            "items": [
              {
                "itemId": "30011",
                "name": "Cafe 200g",
                "sellers": [{
                  "sellerId": "1",
                  "commertialOffer": {
                    "ListPrice": 25.00,
                    "Price": 20.00,
                    "AvailableQuantity": 8,
                    "PromotionTeasers": [],
                    "Teasers": [
                      {
                        "<Name>k__BackingField": "Promo Banco",
                        "<Conditions>k__BackingField": {
                          "<MinimumQuantity>k__BackingField": 1,
                          "<Parameters>k__BackingField": [
                            {"<Name>k__BackingField": "RestrictionsBins", "<Value>k__BackingField": "905051,477357,489068,512533"}
                          ]
                        },
                        "<Effects>k__BackingField": {
                          "<Parameters>k__BackingField": [
                            {"<Name>k__BackingField": "NominalDiscount", "<Value>k__BackingField": "3.00"}
                          ]
                        }
                      }
                    ]
                  }
                }]
              },
              {
                "itemId": "30012",
                "name": "Cafe 100g",
                "sellers": [{
                  "sellerId": "1",
                  "commertialOffer": {
                    "ListPrice": 15.00,
                    "Price": 12.00,
                    "AvailableQuantity": 4,
                    "PromotionTeasers": [],
                    "Teasers": [
                      {
                        "<Name>k__BackingField": "Exclusivo App",
                        "<Conditions>k__BackingField": {
                          "<MinimumQuantity>k__BackingField": 0,
                          "<Parameters>k__BackingField": [
                            {"<Name>k__BackingField": "PaymentMethodId", "<Value>k__BackingField": "208,210"}
                          ]
                        },
                        "<Effects>k__BackingField": {
                          "<Parameters>k__BackingField": [
                            {"<Name>k__BackingField": "CardPrice", "<Value>k__BackingField": "9.90"}
                          ]
                        }
                      }
                    ]
                  }
                }]
              }
            ]
          }
        ]"#;

        let pv = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = pv.parse_page_payload(fixture, &ctx).unwrap();

        assert_eq!(products[0].skus[0].card_price, Some(17.00));
        assert_eq!(
            products[0].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );

        assert_eq!(products[0].skus[1].card_price, Some(9.90));
        assert_eq!(products[0].skus[1].card_program.as_deref(), Some("Oh!"));
    }

    #[test]
    fn vtex_metro_tcenco_sets_cencosud_and_ignores_shipping_and_min_qty_gt_1() {
        let fixture = r#"[
          {
            "productId": "4001",
            "productName": "TV TCL 43",
            "items": [{
              "itemId": "40011",
              "name": "TV TCL 43 FHD",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 899.00,
                  "Price": 599.00,
                  "AvailableQuantity": 6,
                  "PromotionTeasers": [
                    {
                      "Name": "Envio Gratis Metro",
                      "Conditions": {"MinimumQuantity": 0, "Parameters": []},
                      "Effects": {
                        "Parameters": [
                          {"Name": "ShippingPercentualDiscount", "Value": "100"}
                        ]
                      }
                    },
                    {
                      "Name": "Tag-p3x11.50",
                      "Conditions": {"MinimumQuantity": 3, "Parameters": []},
                      "Effects": {
                        "Parameters": [
                          {"Name": "MaximumUnitPriceDiscount", "Value": "1150"}
                        ]
                      }
                    },
                    {
                      "Name": "[TCENCO] Nov26 - Tv - Tcl a 419 Con TC Metro",
                      "Conditions": {"MinimumQuantity": 0, "Parameters": []},
                      "Effects": {
                        "Parameters": [
                          {"Name": "MaximumUnitPriceDiscount", "Value": "41900"}
                        ]
                      }
                    }
                  ]
                }
              }]
            }]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::Metro, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();

        assert_eq!(products[0].skus[0].card_price, Some(419.0));
        assert_eq!(
            products[0].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );
    }

    #[test]
    fn vtex_wong_classifies_tc_wong_cencosud_and_wong_prime_and_tracks_best_program() {
        let fixture = r#"[
          {
            "productId": "5001",
            "productName": "Vino Tinto Reserva",
            "items": [{
              "itemId": "50011",
              "name": "Vino Tinto 750ml",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 60.00,
                  "Price": 50.00,
                  "AvailableQuantity": 12,
                  "PromotionTeasers": [
                    {
                      "Name": "[TCENCO] Descuento Exclusivo Con TC Wong",
                      "Conditions": {"MinimumQuantity": 1, "Parameters": []},
                      "Effects": {
                        "Parameters": [
                          {"Name": "PercentualDiscount", "Value": "10.0"}
                        ]
                      }
                    }
                  ]
                }
              }]
            }]
          },
          {
            "productId": "5002",
            "productName": "Queso Edam",
            "items": [{
              "itemId": "50021",
              "name": "Queso Edam 250g",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 25.00,
                  "Price": 20.00,
                  "AvailableQuantity": 9,
                  "PromotionTeasers": [
                    {
                      "Name": "[TCENCO] Con TC Wong 10%",
                      "Effects": {
                        "Parameters": [
                          {"Name": "PercentualDiscount", "Value": "10.0"}
                        ]
                      }
                    },
                    {
                      "Name": "Wong Prime 15%",
                      "Effects": {
                        "Parameters": [
                          {"Name": "PercentualDiscount", "Value": "15.0"}
                        ]
                      }
                    }
                  ]
                }
              }]
            }]
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::Wong, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter.parse_page_payload(fixture, &ctx).unwrap();

        assert_eq!(products[0].skus[0].card_price, Some(45.0));
        assert_eq!(
            products[0].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );

        assert_eq!(products[1].skus[0].card_price, Some(17.0));
        assert_eq!(
            products[1].skus[0].card_program.as_deref(),
            Some("Wong Prime")
        );
    }

    #[test]
    fn vtex_supports_camelcase_teasers_retailer_fallbacks_and_codigo_de_barras_spec() {
        let fixture = r#"[
          {
            "productId": "6001",
            "productName": "Mantequilla Gloria",
            "Código de barras": ["7751271034142"],
            "items": [{
              "itemId": "60011",
              "ean": "",
              "name": "Mantequilla 200g",
              "sellers": [{
                "sellerId": "1",
                "commertialOffer": {
                  "ListPrice": 12.00,
                  "Price": 10.00,
                  "AvailableQuantity": 5,
                  "teasers": [
                    {
                      "name": "Solo 2 unidades",
                      "conditions": {"minimumQuantity": 2, "parameters": []},
                      "effects": {
                        "parameters": [{"name": "PromotionalPrice", "value": "5.00"}]
                      }
                    },
                    {
                      "name": "Promocion Tarjeta Tienda",
                      "conditions": {
                        "minimumQuantity": 1,
                        "parameters": [{"name": "PaymentMethodId", "value": "202"}]
                      },
                      "effects": {
                        "parameters": [{"name": "PromotionalPrice", "value": "8.50"}]
                      }
                    }
                  ]
                }
              }]
            }]
          }
        ]"#;

        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);

        let pv = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let pv_products = pv.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(pv_products[0].skus[0].ean.as_deref(), Some("7751271034142"));
        assert_eq!(pv_products[0].skus[0].card_price, Some(8.50));
        assert_eq!(pv_products[0].skus[0].card_program.as_deref(), Some("Oh!"));

        // Same PaymentMethodId "202" on Metro/Wong must NOT classify as "Oh!" and falls back to "Cencosud".
        let metro = VtexAdapter::new(VtexRetailer::Metro, test_http());
        let metro_products = metro.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(metro_products[0].skus[0].card_price, Some(8.50));
        assert_eq!(
            metro_products[0].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );

        let wong = VtexAdapter::new(VtexRetailer::Wong, test_http());
        let wong_products = wong.parse_page_payload(fixture, &ctx).unwrap();
        assert_eq!(wong_products[0].skus[0].card_price, Some(8.50));
        assert_eq!(
            wong_products[0].skus[0].card_program.as_deref(),
            Some("Cencosud")
        );
    }

    #[test]
    fn vtex_product_carries_query_term_and_content_hash() {
        let adapter = VtexAdapter::new(VtexRetailer::Wong, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None).with_query_term("lacteos");

        let body_a = r#"[{
          "productId": "7001",
          "productName": "Leche Fresca",
          "items": [{
            "itemId": "70011",
            "name": "Leche Fresca 1L",
            "sellers": [{
              "sellerId": "1",
              "commertialOffer": {
                "Price": 5.90,
                "PriceToken": "token_one",
                "PriceValidUntil": "2026-11-14T01:00:00Z",
                "CacheVersionUsedToCallCheckout": "cache_1"
              }
            }]
          }]
        }]"#;
        let body_b = r#"[{
          "productId": "7001",
          "productName": "Leche Fresca",
          "items": [{
            "itemId": "70011",
            "name": "Leche Fresca 1L",
            "sellers": [{
              "sellerId": "1",
              "commertialOffer": {
                "Price": 5.90,
                "PriceToken": "token_two",
                "PriceValidUntil": "2026-11-14T01:30:00Z",
                "CacheVersionUsedToCallCheckout": "cache_2"
              }
            }]
          }]
        }]"#;

        let pa = adapter.parse_page_payload(body_a, &ctx).unwrap();
        let pb = adapter.parse_page_payload(body_b, &ctx).unwrap();
        assert_eq!(pa.len(), 1);
        assert_eq!(pb.len(), 1);
        assert_eq!(pa[0].query_term, "lacteos");
        assert_eq!(pb[0].query_term, "lacteos");
        assert_eq!(pa[0].content_hash, pb[0].content_hash);
        assert_ne!(pa[0].payload_hash, pb[0].payload_hash);
    }
}
