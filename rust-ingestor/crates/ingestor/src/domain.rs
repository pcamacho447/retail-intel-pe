use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SkuItem {
    pub sku_id: String,
    pub ean: Option<String>,
    pub seller_id: Option<String>,
    pub raw_name: String,
    pub image_url: Option<String>,
    pub list_price: Option<f64>,
    pub selling_price: Option<f64>,
    pub card_price: Option<f64>,
    pub available_qty: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Product {
    pub retailer: String,
    pub market_id: String,
    pub store_id: Option<String>,
    pub product_id: String,
    pub raw_name: String,
    pub brand: Option<String>,
    pub category_path: Option<String>,
    pub raw_description: Option<String>,
    pub product_url: Option<String>,
    pub currency: String,
    pub sales_channel: String,
    pub postal_code: Option<String>,
    pub scraped_at: DateTime<Utc>,
    pub extraction_run_id: Uuid,
    pub extractor_version: String,
    pub source_endpoint: String,
    pub http_status: u16,
    pub payload_hash: String,
    pub raw_payload: String,
    pub skus: Vec<SkuItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BronzeSkuRow {
    pub retailer: String,
    pub market_id: String,
    pub store_id: Option<String>,
    pub sku_id: String,
    pub product_id: String,
    pub ean: Option<String>,
    pub seller_id: Option<String>,
    pub raw_name: String,
    pub brand: Option<String>,
    pub category_path: Option<String>,
    pub raw_description: Option<String>,
    pub product_url: Option<String>,
    pub image_url: Option<String>,
    pub list_price: Option<f64>,
    pub selling_price: Option<f64>,
    pub card_price: Option<f64>,
    pub available_qty: Option<i64>,
    pub currency: String,
    pub sales_channel: String,
    pub postal_code: Option<String>,
    pub scraped_at: DateTime<Utc>,
    pub extraction_run_id: String,
    pub extractor_version: String,
    pub source_endpoint: String,
    pub http_status: u16,
    pub payload_hash: String,
    pub is_duplicate_payload: bool,
    pub raw_payload: Option<String>,
}

pub fn normalize_opt_string(s: Option<&str>) -> Option<String> {
    s.map(str::trim)
        .filter(|trimmed| !trimmed.is_empty())
        .map(str::to_string)
}

impl Product {
    /// Expands this `Product` (1) into `N` `BronzeSkuRow` records (one per `SkuItem`).
    /// When `is_duplicate_payload` is true, `raw_payload` is omitted (`None`) to avoid
    /// storing duplicate JSON payloads across Bronze partitions.
    pub fn to_bronze_rows(&self, is_duplicate_payload: bool) -> Vec<BronzeSkuRow> {
        if self.skus.is_empty() {
            tracing::warn!(
                retailer = %self.retailer,
                product_id = %self.product_id,
                "Product has 0 SKUs; omitting from BronzeSkuRow expansion"
            );
            return Vec::new();
        }

        let raw_payload = if is_duplicate_payload {
            None
        } else {
            Some(self.raw_payload.clone())
        };
        let run_id_str = self.extraction_run_id.to_string();

        self.skus
            .iter()
            .map(|sku| {
                let raw_name = normalize_opt_string(Some(&sku.raw_name))
                    .unwrap_or_else(|| self.raw_name.clone());
                BronzeSkuRow {
                    retailer: self.retailer.clone(),
                    market_id: self.market_id.clone(),
                    store_id: self.store_id.clone(),
                    sku_id: sku.sku_id.clone(),
                    product_id: self.product_id.clone(),
                    ean: normalize_opt_string(sku.ean.as_deref()),
                    seller_id: normalize_opt_string(sku.seller_id.as_deref()),
                    raw_name,
                    brand: self.brand.clone(),
                    category_path: self.category_path.clone(),
                    raw_description: self.raw_description.clone(),
                    product_url: self.product_url.clone(),
                    image_url: normalize_opt_string(sku.image_url.as_deref()),
                    list_price: sku.list_price,
                    selling_price: sku.selling_price,
                    card_price: sku.card_price,
                    available_qty: sku.available_qty,
                    currency: self.currency.clone(),
                    sales_channel: self.sales_channel.clone(),
                    postal_code: self.postal_code.clone(),
                    scraped_at: self.scraped_at,
                    extraction_run_id: run_id_str.clone(),
                    extractor_version: self.extractor_version.clone(),
                    source_endpoint: self.source_endpoint.clone(),
                    http_status: self.http_status,
                    payload_hash: self.payload_hash.clone(),
                    is_duplicate_payload,
                    raw_payload: raw_payload.clone(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_product(skus: Vec<SkuItem>) -> Product {
        Product {
            retailer: "plazavea".to_string(),
            market_id: "PE".to_string(),
            store_id: Some("lima-01".to_string()),
            product_id: "prod-100".to_string(),
            raw_name: "Leche Gloria Entera".to_string(),
            brand: Some("Gloria".to_string()),
            category_path: Some("/Lacteos/Leches/".to_string()),
            raw_description: Some("Lata 390g".to_string()),
            product_url: Some("https://www.plazavea.com.pe/leche/p".to_string()),
            currency: "PEN".to_string(),
            sales_channel: "ecommerce".to_string(),
            postal_code: Some("150101".to_string()),
            scraped_at: Utc::now(),
            extraction_run_id: Uuid::new_v4(),
            extractor_version: "vtex-catalog-v1.0.0".to_string(),
            source_endpoint:
                "https://www.plazavea.com.pe/api/catalog_system/pub/products/search/lacteos"
                    .to_string(),
            http_status: 200,
            payload_hash: "abc123def456".to_string(),
            raw_payload: r#"{"productId":"prod-100"}"#.to_string(),
            skus,
        }
    }

    #[test]
    fn normalize_opt_string_converts_empty_and_whitespace_to_none() {
        assert_eq!(normalize_opt_string(None), None);
        assert_eq!(normalize_opt_string(Some("")), None);
        assert_eq!(normalize_opt_string(Some("   \t\n ")), None);
        assert_eq!(
            normalize_opt_string(Some("  7750243012345  ")),
            Some("7750243012345".to_string())
        );
    }

    #[test]
    fn product_to_bronze_rows_expands_one_to_many_and_preserves_payload_when_new() {
        let product = sample_product(vec![
            SkuItem {
                sku_id: "sku-1".to_string(),
                ean: Some("7750243012345".to_string()),
                seller_id: Some("1".to_string()),
                raw_name: "Leche Gloria 390g Unidad".to_string(),
                image_url: Some("https://img.example.com/1.jpg".to_string()),
                list_price: Some(4.50),
                selling_price: Some(4.20),
                card_price: Some(3.99),
                available_qty: Some(50),
            },
            SkuItem {
                sku_id: "sku-2".to_string(),
                ean: Some("   ".to_string()),
                seller_id: None,
                raw_name: "Leche Gloria 390g Sixpack".to_string(),
                image_url: None,
                list_price: Some(25.0),
                selling_price: Some(23.9),
                card_price: None,
                available_qty: Some(12),
            },
        ]);

        let rows = product.to_bronze_rows(false);
        assert_eq!(rows.len(), 2);

        assert_eq!(rows[0].sku_id, "sku-1");
        assert_eq!(rows[0].product_id, "prod-100");
        assert_eq!(rows[0].ean, Some("7750243012345".to_string()));
        assert_eq!(rows[0].payload_hash, "abc123def456");
        assert!(!rows[0].is_duplicate_payload);
        assert_eq!(
            rows[0].raw_payload,
            Some(r#"{"productId":"prod-100"}"#.to_string())
        );
        assert_eq!(
            rows[0].extraction_run_id,
            product.extraction_run_id.to_string()
        );

        assert_eq!(rows[1].sku_id, "sku-2");
        assert_eq!(rows[1].product_id, "prod-100");
        assert_eq!(rows[1].ean, None);
        assert_eq!(rows[1].card_price, None);
        assert!(!rows[1].is_duplicate_payload);
        assert_eq!(
            rows[1].raw_payload,
            Some(r#"{"productId":"prod-100"}"#.to_string())
        );
    }

    #[test]
    fn product_to_bronze_rows_omits_raw_payload_when_duplicate() {
        let product = sample_product(vec![SkuItem {
            sku_id: "sku-1".to_string(),
            ean: None,
            seller_id: Some("1".to_string()),
            raw_name: "Leche Gloria 390g".to_string(),
            image_url: None,
            list_price: Some(4.50),
            selling_price: Some(4.20),
            card_price: None,
            available_qty: Some(10),
        }]);

        let rows = product.to_bronze_rows(true);
        assert_eq!(rows.len(), 1);
        assert!(rows[0].is_duplicate_payload);
        assert_eq!(rows[0].raw_payload, None);
        assert_eq!(rows[0].payload_hash, "abc123def456");
    }

    #[test]
    fn product_to_bronze_rows_handles_empty_skus_gracefully() {
        let product = sample_product(vec![]);
        let rows = product.to_bronze_rows(false);
        assert!(rows.is_empty());
    }
}
