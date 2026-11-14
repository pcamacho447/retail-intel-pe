use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::Product;

#[derive(Debug, Clone)]
pub struct FetchContext {
    pub market_id: String,
    pub store_id: Option<String>,
    pub postal_code: Option<String>,
    pub sales_channel: String,
    pub currency: String,
    pub extraction_run_id: Uuid,
    pub source_endpoint: String,
    pub http_status: u16,
    pub scraped_at: DateTime<Utc>,
    pub query_term: String,
}

impl FetchContext {
    #[must_use]
    pub fn for_run(
        extraction_run_id: Uuid,
        market_id: impl Into<String>,
        postal_code: Option<String>,
    ) -> Self {
        Self {
            market_id: market_id.into(),
            store_id: None,
            postal_code,
            sales_channel: "ecommerce".to_string(),
            currency: "PEN".to_string(),
            extraction_run_id,
            source_endpoint: String::new(),
            http_status: 200,
            scraped_at: Utc::now(),
            query_term: String::new(),
        }
    }

    #[must_use]
    pub fn with_endpoint(mut self, endpoint: impl Into<String>, http_status: u16) -> Self {
        self.source_endpoint = endpoint.into();
        self.http_status = http_status;
        self.scraped_at = Utc::now();
        self
    }

    pub fn with_query_term(mut self, term: impl Into<String>) -> Self {
        self.query_term = term.into();
        self
    }
}

#[derive(Debug, Clone)]
pub struct PageBatch {
    pub products: Vec<Product>,
    pub has_more: bool,
    pub next_offset: usize,
    pub raw_bytes: usize,
    pub http_status: u16,
    pub latency_ms: f64,
    pub products_rejected: usize,
}

#[async_trait]
pub trait RetailerAdapter: Send + Sync {
    fn retailer_slug(&self) -> &'static str;
    fn extractor_version(&self) -> &'static str;
    fn build_page_url(&self, category_or_query: &str, offset: usize, page_size: usize) -> String;
    fn parse_page_payload(&self, body: &str, ctx: &FetchContext) -> anyhow::Result<Vec<Product>>;
    async fn fetch_page(
        &self,
        category_or_query: &str,
        offset: usize,
        page_size: usize,
        ctx: &FetchContext,
    ) -> anyhow::Result<PageBatch>;

    fn volatile_payload_keys(&self) -> &'static [&'static str] {
        &[]
    }

    fn applies_geo_context(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct DummyAdapter;

    #[async_trait]
    impl RetailerAdapter for DummyAdapter {
        fn retailer_slug(&self) -> &'static str {
            "dummy"
        }

        fn extractor_version(&self) -> &'static str {
            "0.1.0"
        }

        fn build_page_url(
            &self,
            category_or_query: &str,
            offset: usize,
            page_size: usize,
        ) -> String {
            format!("https://example.com/{category_or_query}?offset={offset}&limit={page_size}")
        }

        fn parse_page_payload(
            &self,
            _body: &str,
            _ctx: &FetchContext,
        ) -> anyhow::Result<Vec<Product>> {
            Ok(vec![])
        }

        async fn fetch_page(
            &self,
            category_or_query: &str,
            offset: usize,
            page_size: usize,
            ctx: &FetchContext,
        ) -> anyhow::Result<PageBatch> {
            let endpoint = self.build_page_url(category_or_query, offset, page_size);
            let page_ctx = ctx.clone().with_endpoint(endpoint, 200);
            let products = self.parse_page_payload("[]", &page_ctx)?;
            Ok(PageBatch {
                products,
                has_more: false,
                next_offset: offset + page_size,
                raw_bytes: 2,
                http_status: 200,
                latency_ms: 1.0,
                products_rejected: 0,
            })
        }
    }

    #[test]
    fn fetch_context_sets_defaults_and_endpoint() {
        let run_id = Uuid::new_v4();
        let ctx = FetchContext::for_run(run_id, "PE", Some("150101".to_string()))
            .with_endpoint("https://www.plazavea.com.pe/api/test", 206);

        assert_eq!(ctx.extraction_run_id, run_id);
        assert_eq!(ctx.market_id, "PE");
        assert_eq!(ctx.postal_code.as_deref(), Some("150101"));
        assert_eq!(ctx.currency, "PEN");
        assert_eq!(ctx.sales_channel, "ecommerce");
        assert_eq!(ctx.source_endpoint, "https://www.plazavea.com.pe/api/test");
        assert_eq!(ctx.http_status, 206);
        assert_eq!(ctx.query_term, "");

        let ctx_with_term = ctx.with_query_term("lacteos");
        assert_eq!(ctx_with_term.query_term, "lacteos");
    }

    #[tokio::test]
    async fn retailer_adapter_trait_is_object_safe() {
        let adapter: Box<dyn RetailerAdapter> = Box::new(DummyAdapter);
        assert_eq!(adapter.retailer_slug(), "dummy");
        assert_eq!(adapter.extractor_version(), "0.1.0");
        assert!(adapter.volatile_payload_keys().is_empty());
        assert!(!adapter.applies_geo_context());
        assert_eq!(
            adapter.build_page_url("lacteos", 0, 20),
            "https://example.com/lacteos?offset=0&limit=20"
        );

        let ctx = FetchContext::for_run(Uuid::new_v4(), "PE", None);
        let batch = adapter.fetch_page("lacteos", 0, 20, &ctx).await.unwrap();
        assert_eq!(batch.http_status, 200);
        assert!(!batch.has_more);
        assert_eq!(batch.next_offset, 20);
        assert_eq!(batch.products_rejected, 0);
    }
}
