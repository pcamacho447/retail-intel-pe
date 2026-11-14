#![allow(
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::too_many_lines
)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::bail;
use clap::{Parser, ValueEnum};

use ingestor::adapter::RetailerAdapter;
use ingestor::adapters::falabella::FalabellaListingAdapter;
use ingestor::adapters::vtex::{VtexAdapter, VtexRetailer};
use ingestor::domain::normalize_opt_string;
use ingestor::http::RateLimitedHttpClient;
use ingestor::runner::{run_ingest, IngestConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RetailerArg {
    #[value(name = "plazavea")]
    PlazaVea,
    #[value(name = "metro")]
    Metro,
    #[value(name = "wong")]
    Wong,
    #[value(name = "tottus")]
    Tottus,
}

impl RetailerArg {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PlazaVea => "plazavea",
            Self::Metro => "metro",
            Self::Wong => "wong",
            Self::Tottus => "tottus",
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "ingestor",
    version,
    about = "Retail Pricing Intelligence Peru (E1) multi-retailer Bronze ingestor"
)]
pub struct Cli {
    #[arg(long, value_enum)]
    pub retailer: RetailerArg,

    #[arg(long, value_delimiter = ',', required = true)]
    pub categories: Vec<String>,

    #[arg(long, default_value_t = 20)]
    pub page_size: usize,

    #[arg(long, default_value_t = 2)]
    pub max_pages: usize,

    #[arg(long, default_value = "data/bronze")]
    pub output_dir: PathBuf,

    #[arg(long, default_value_t = false)]
    pub resume: bool,

    #[arg(long, default_value = "PE")]
    pub market_id: String,

    #[arg(long)]
    pub postal_code: Option<String>,

    #[arg(long, default_value_t = false)]
    pub enrich_tottus_pdp: bool,

    #[arg(long, default_value = "state/tottus_ean_cache.json")]
    pub tottus_ean_cache: PathBuf,
}

pub fn build_adapter(
    retailer: RetailerArg,
    http: Arc<RateLimitedHttpClient>,
) -> Box<dyn RetailerAdapter> {
    match retailer {
        RetailerArg::PlazaVea => Box::new(VtexAdapter::new(VtexRetailer::PlazaVea, http)),
        RetailerArg::Metro => Box::new(VtexAdapter::new(VtexRetailer::Metro, http)),
        RetailerArg::Wong => Box::new(VtexAdapter::new(VtexRetailer::Wong, http)),
        RetailerArg::Tottus => Box::new(FalabellaListingAdapter::new_tottus(http)),
    }
}

pub fn build_adapter_for_cli(
    cli: &Cli,
    http: Arc<RateLimitedHttpClient>,
) -> Box<dyn RetailerAdapter> {
    match cli.retailer {
        RetailerArg::Tottus => {
            let cache_path = (!cli.tottus_ean_cache.as_os_str().is_empty())
                .then(|| cli.tottus_ean_cache.clone());
            Box::new(FalabellaListingAdapter::new_tottus_with_enrichment(
                http,
                cli.enrich_tottus_pdp,
                cache_path,
            ))
        }
        other => build_adapter(other, http),
    }
}

pub fn cli_to_config(cli: &Cli) -> anyhow::Result<IngestConfig> {
    let categories: Vec<String> = cli
        .categories
        .iter()
        .filter_map(|c| normalize_opt_string(Some(c)))
        .collect();
    if categories.is_empty() {
        bail!("--categories must contain at least one non-empty category or query");
    }
    if cli.page_size == 0 {
        bail!("--page-size must be greater than 0");
    }
    if cli.max_pages == 0 {
        bail!("--max-pages must be greater than 0");
    }
    let market_id = cli.market_id.trim();
    if market_id.is_empty() {
        bail!("--market-id must not be empty");
    }

    Ok(IngestConfig {
        retailer: cli.retailer.as_str().to_string(),
        categories,
        page_size: cli.page_size,
        max_pages: cli.max_pages,
        output_dir: cli.output_dir.clone(),
        resume: cli.resume,
        market_id: market_id.to_string(),
        postal_code: normalize_opt_string(cli.postal_code.as_deref()),
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    let config = cli_to_config(&cli)?;
    let http = Arc::new(RateLimitedHttpClient::new()?);
    let adapter = build_adapter_for_cli(&cli, http.clone());

    let metrics = run_ingest(adapter.as_ref(), &http, &config).await?;
    println!("{}", serde_json::to_string_pretty(&metrics)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses_required_and_optional_flags() {
        let cli = Cli::try_parse_from([
            "ingestor",
            "--retailer",
            "plazavea",
            "--categories",
            "lacteos, abarrotes",
            "--page-size",
            "15",
            "--max-pages",
            "3",
            "--output-dir",
            "/tmp/bronze",
            "--resume",
            "--market-id",
            "PE",
            "--postal-code",
            "150101",
        ])
        .unwrap();

        let cfg = cli_to_config(&cli).unwrap();
        assert_eq!(cfg.retailer, "plazavea");
        assert_eq!(cfg.categories, vec!["lacteos", "abarrotes"]);
        assert_eq!(cfg.page_size, 15);
        assert_eq!(cfg.max_pages, 3);
        assert_eq!(cfg.output_dir, PathBuf::from("/tmp/bronze"));
        assert!(cfg.resume);
        assert_eq!(cfg.market_id, "PE");
        assert_eq!(cfg.postal_code.as_deref(), Some("150101"));
    }

    #[test]
    fn cli_uses_expected_defaults() {
        let cli =
            Cli::try_parse_from(["ingestor", "--retailer", "tottus", "--categories", "leche"])
                .unwrap();
        let cfg = cli_to_config(&cli).unwrap();
        assert_eq!(cfg.retailer, "tottus");
        assert_eq!(cfg.page_size, 20);
        assert_eq!(cfg.max_pages, 2);
        assert_eq!(cfg.output_dir, PathBuf::from("data/bronze"));
        assert!(!cfg.resume);
        assert_eq!(cfg.market_id, "PE");
        assert_eq!(cfg.postal_code, None);
    }

    #[test]
    fn build_adapter_instantiates_all_four_retailers() {
        let http = Arc::new(RateLimitedHttpClient::new().unwrap());
        assert_eq!(
            build_adapter(RetailerArg::PlazaVea, http.clone()).retailer_slug(),
            "plazavea"
        );
        assert_eq!(
            build_adapter(RetailerArg::Metro, http.clone()).retailer_slug(),
            "metro"
        );
        assert_eq!(
            build_adapter(RetailerArg::Wong, http.clone()).retailer_slug(),
            "wong"
        );
        assert_eq!(
            build_adapter(RetailerArg::Tottus, http).retailer_slug(),
            "tottus"
        );
    }

    #[test]
    fn cli_rejects_zero_max_pages_and_blank_market_id() {
        let zero_pages = Cli::try_parse_from([
            "ingestor",
            "--retailer",
            "plazavea",
            "--categories",
            "lacteos",
            "--max-pages",
            "0",
        ])
        .unwrap();
        assert!(cli_to_config(&zero_pages).is_err());

        let blank_market = Cli::try_parse_from([
            "ingestor",
            "--retailer",
            "plazavea",
            "--categories",
            "lacteos",
            "--market-id",
            "   ",
        ])
        .unwrap();
        assert!(cli_to_config(&blank_market).is_err());
    }

    #[test]
    fn cli_parses_tottus_enrichment_flags_and_builds_adapter() {
        let default_cli =
            Cli::try_parse_from(["ingestor", "--retailer", "tottus", "--categories", "leche"])
                .unwrap();
        assert!(!default_cli.enrich_tottus_pdp);
        assert_eq!(
            default_cli.tottus_ean_cache,
            PathBuf::from("state/tottus_ean_cache.json")
        );

        let enriched_cli = Cli::try_parse_from([
            "ingestor",
            "--retailer",
            "tottus",
            "--categories",
            "leche,abarrotes",
            "--enrich-tottus-pdp",
            "--tottus-ean-cache",
            "/tmp/custom_tottus_cache.json",
        ])
        .unwrap();
        assert!(enriched_cli.enrich_tottus_pdp);
        assert_eq!(
            enriched_cli.tottus_ean_cache,
            PathBuf::from("/tmp/custom_tottus_cache.json")
        );

        let http = Arc::new(RateLimitedHttpClient::new().unwrap());
        let adapter = build_adapter_for_cli(&enriched_cli, http);
        assert_eq!(adapter.retailer_slug(), "tottus");
    }
}
