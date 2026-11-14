use std::sync::Arc;

use anyhow::{anyhow, Context};
use async_trait::async_trait;
use serde_json::Value;

use crate::adapter::{FetchContext, PageBatch, RetailerAdapter};
use crate::adapters::{absolutize_url, encode_url_segment, value_as_string};
use crate::dedup::compute_payload_hash;
use crate::domain::{normalize_opt_string, Product, SkuItem};
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
    if num.is_finite() && num > 0.0 {
        Some(num)
    } else {
        None
    }
}

fn extract_card_price(
    offer: &Value,
    sku_id: &str,
    list_price: Option<f64>,
    selling_price: Option<f64>,
) -> Option<f64> {
    let mut best: Option<f64> = None;

    for key in ["PromotionTeasers", "Teasers"] {
        let Some(teasers) = offer.get(key).and_then(|v| v.as_array()) else {
            continue;
        };
        for teaser in teasers {
            let min_qty = teaser
                .get("Conditions")
                .or_else(|| teaser.get("conditions"))
                .and_then(|c| {
                    c.get("MinimumQuantity")
                        .or_else(|| c.get("minimumQuantity"))
                })
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            if min_qty > 1 {
                continue;
            }

            let params = teaser
                .get("Effects")
                .or_else(|| teaser.get("effects"))
                .and_then(|e| e.get("Parameters").or_else(|| e.get("parameters")))
                .and_then(|p| p.as_array());

            let Some(params) = params else {
                continue;
            };

            let target_ids_param = params.iter().find(|param| {
                param
                    .get("Name")
                    .or_else(|| param.get("name"))
                    .and_then(|v| v.as_str())
                    .is_some_and(|n| n.eq_ignore_ascii_case("PromotionalPriceTableItemsIds"))
            });
            if let Some(ids_val) = target_ids_param.and_then(|p| {
                p.get("Value")
                    .or_else(|| p.get("value"))
                    .and_then(|v| v.as_str())
            }) {
                let targets_sku = ids_val
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .any(|id| id == sku_id);
                if !ids_val.trim().is_empty() && !targets_sku {
                    continue;
                }
            }

            for param in params {
                let name = param
                    .get("Name")
                    .or_else(|| param.get("name"))
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_ascii_lowercase();
                if name.contains("shipping") {
                    continue;
                }

                let raw_val = param.get("Value").or_else(|| param.get("value"));
                let parsed_num = raw_val.and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str()?.trim().parse::<f64>().ok())
                });

                let Some(val) = parsed_num.filter(|x| x.is_finite() && *x > 0.0) else {
                    continue;
                };

                let candidate = if name == "promotionalpricetableitemsdiscount" {
                    list_price
                        .or(selling_price)
                        .filter(|base| *base > val)
                        .map(|base| ((base - val) * 100.0).round() / 100.0)
                        .filter(|p| selling_price.is_none_or(|sp| *p < sp))
                } else if name == "maximumunitpricediscount" {
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

                if let Some(price) = candidate.filter(|p| *p > 0.0) {
                    best = Some(best.map_or(price, |curr| curr.min(price)));
                }
            }
        }
    }

    best
}

fn parse_vtex_sku(item: &Value, fallback_name: &str) -> Option<SkuItem> {
    let sku_id = value_as_string(item.get("itemId"))?;
    let ean = normalize_opt_string(item.get("ean").and_then(|v| v.as_str()));
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

    let (seller_id, list_price, selling_price, card_price, available_qty) =
        if let Some(seller) = selected_seller {
            let seller_id = value_as_string(seller.get("sellerId"));
            let offer = seller.get("commertialOffer").unwrap_or(&Value::Null);
            let list_price = positive_f64(offer.get("ListPrice"));
            let selling_price = positive_f64(offer.get("Price"));
            let card_price = extract_card_price(offer, &sku_id, list_price, selling_price);
            let available_qty = offer
                .get("AvailableQuantity")
                .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)));
            (
                seller_id,
                list_price,
                selling_price,
                card_price,
                available_qty,
            )
        } else {
            (None, None, None, None, None)
        };

    Some(SkuItem {
        sku_id,
        ean,
        seller_id,
        raw_name,
        image_url,
        list_price,
        selling_price,
        card_price,
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
        for elem in items {
            let Some(product_id) = value_as_string(elem.get("productId")) else {
                tracing::warn!(
                    retailer = %self.retailer_slug(),
                    "VTEX product object missing productId; skipping item"
                );
                continue;
            };
            let raw_payload = serde_json::to_string(elem)?;
            let payload_hash = compute_payload_hash(&raw_payload);
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
                        .filter_map(|sku| parse_vtex_sku(sku, &raw_name))
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
        let mut url = self.build_page_url(category_or_query, offset, effective_size);
        if let Some(ref pc) = ctx.postal_code {
            if !url.contains("_postalCode=") {
                let sep = if url.contains('?') { '&' } else { '?' };
                url.push_str(&format!("{sep}_postalCode={}", encode_url_segment(pc)));
            }
        }
        if let Some(ref sid) = ctx.store_id {
            if !url.contains("_storeId=") {
                let sep = if url.contains('?') { '&' } else { '?' };
                url.push_str(&format!("{sep}_storeId={}", encode_url_segment(sid)));
            }
        }
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
        assert_eq!(p1.skus[0].available_qty, Some(35));

        assert_eq!(p1.skus[1].sku_id, "1002");
        assert_eq!(p1.skus[1].ean, None);
        assert_eq!(p1.skus[1].card_price, None);
        assert_eq!(p1.skus[1].selling_price, Some(24.90));

        let p2 = &products[1];
        assert_eq!(p2.raw_description, None);
        assert_eq!(p2.skus[0].ean, None);
        assert_eq!(p2.skus[0].seller_id, None);
        assert_eq!(p2.skus[0].list_price, None);
        assert_eq!(p2.skus[0].selling_price, None);
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
        assert_eq!(products[1].skus[0].card_price, Some(1199.00));
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

    #[tokio::test]
    async fn vtex_fetch_page_appends_postal_code_and_store_id_to_endpoint() {
        let body = r#"[{"productId":"1","productName":"Item 1","items":[{"itemId":"11","name":"Item 1","sellers":[]}]}]"#;
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

        let adapter = VtexAdapter::with_base_url(
            VtexRetailer::PlazaVea,
            format!("http://{addr}"),
            test_http(),
        );
        let mut ctx = FetchContext::for_run(Uuid::new_v4(), "PE", Some("15001".to_string()));
        ctx.store_id = Some("TIENDA_1".to_string());
        let batch = adapter.fetch_page("lacteos", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch.products.len(), 1);

        let req = req_rx.recv().await.unwrap();
        assert!(
            req.contains("_postalCode=15001"),
            "request missing _postalCode: {req}"
        );
        assert!(
            req.contains("_storeId=TIENDA_1"),
            "request missing _storeId: {req}"
        );

        let endpoint = &batch.products[0].source_endpoint;
        assert!(
            endpoint.contains("_postalCode=15001"),
            "product endpoint missing _postalCode: {endpoint}"
        );
        assert!(
            endpoint.contains("_storeId=TIENDA_1"),
            "product endpoint missing _storeId: {endpoint}"
        );
    }

    #[test]
    fn vtex_skips_item_missing_product_id_and_parses_valid_items() {
        let fixture = r#"[
          {
            "productName": "Invalid Product Without Id",
            "items": []
          },
          {
            "productId": "888",
            "productName": "Valid Product",
            "items": []
          }
        ]"#;

        let adapter = VtexAdapter::new(VtexRetailer::PlazaVea, test_http());
        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let products = adapter
            .parse_page_payload(fixture, &ctx)
            .expect("should succeed");
        assert_eq!(products.len(), 1);
        assert_eq!(products[0].product_id, "888");
    }
}
