-- =============================================================================
-- retail-intel-pe: PostgreSQL Operational & Master Catalog Schema (Phase E1)
-- Idempotent DDL for extraction run telemetry, canonical product master
-- catalog, SCD Type 2 SKU-to-Trade-Item bridge, and human review audit log.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Operational Telemetry: Extraction Runs (Bronze Ingestion E1)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS extraction_runs (
    extraction_run_id   UUID PRIMARY KEY,
    retailer            VARCHAR(32) NOT NULL,
    extractor_version   VARCHAR(32) NOT NULL,
    started_at          TIMESTAMPTZ NOT NULL,
    finished_at         TIMESTAMPTZ,
    status              VARCHAR(24) NOT NULL,
    products_recovered  INTEGER NOT NULL DEFAULT 0,
    skus_recovered      INTEGER NOT NULL DEFAULT 0,
    pages_fetched       INTEGER NOT NULL DEFAULT 0,
    duplicates_detected INTEGER NOT NULL DEFAULT 0,
    requests_ok         INTEGER NOT NULL DEFAULT 0,
    requests_error      INTEGER NOT NULL DEFAULT 0,
    avg_latency_ms      NUMERIC(10,2),
    mb_transferred      NUMERIC(12,4),
    total_duration_secs NUMERIC(10,2),
    peak_ram_mb         NUMERIC(10,2),
    parquet_path        TEXT
);

CREATE INDEX IF NOT EXISTS idx_extraction_runs_retailer
    ON extraction_runs (retailer);

CREATE INDEX IF NOT EXISTS idx_extraction_runs_started_at
    ON extraction_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_extraction_runs_retailer_started_at
    ON extraction_runs (retailer, started_at DESC);

CREATE INDEX IF NOT EXISTS idx_extraction_runs_status
    ON extraction_runs (status);

-- -----------------------------------------------------------------------------
-- 2. Master Catalog: Product Family (Conceptual Product Line)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product_family (
    family_id       UUID PRIMARY KEY,
    brand           VARCHAR(128),
    product_type    VARCHAR(128) NOT NULL,
    category_id     VARCHAR(64),
    normalized_name VARCHAR(255) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_product_family_category
    ON dim_product_family (category_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_family_brand_type
    ON dim_product_family (brand, product_type);

-- -----------------------------------------------------------------------------
-- 3. Master Catalog: Canonical Trade Item (Exact Sellable Unit / Pack)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_trade_item (
    trade_item_id           UUID PRIMARY KEY,
    family_id               UUID REFERENCES dim_product_family(family_id),
    gtin                    VARCHAR(14) UNIQUE,
    brand                   VARCHAR(128),
    commercial_description  TEXT NOT NULL,
    net_quantity            NUMERIC(12,4),
    net_quantity_unit       VARCHAR(32),
    pack_units              INTEGER NOT NULL DEFAULT 1 CHECK (pack_units >= 1),
    presentation            VARCHAR(64),
    product_form            VARCHAR(64),
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Intentionally omitted idx_dim_trade_item_gtin: `gtin VARCHAR(14) UNIQUE` above
-- already creates a full unique B-tree index in PostgreSQL (and allows NULLs).

CREATE INDEX IF NOT EXISTS idx_dim_trade_item_family_id
    ON dim_trade_item (family_id);

-- -----------------------------------------------------------------------------
-- 4. Master Catalog: Comparison Group (Unit-Price & Substitution Equivalency)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_comparison_group (
    comparison_group_id UUID PRIMARY KEY,
    category_id         VARCHAR(64) NOT NULL,
    comparison_unit     VARCHAR(32) NOT NULL,
    comparability_rule  JSONB NOT NULL,
    group_description   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_dim_comparison_group_category
    ON dim_comparison_group (category_id);

CREATE INDEX IF NOT EXISTS idx_dim_comparison_group_rule_gin
    ON dim_comparison_group USING GIN (comparability_rule);

-- -----------------------------------------------------------------------------
-- 5. Master Catalog: Bridge SKU -> Trade Item (SCD Type 2 Versioned Mapping)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bridge_sku_trade_item (
    bridge_id        BIGSERIAL PRIMARY KEY,
    retailer         VARCHAR(32) NOT NULL,
    market_id        VARCHAR(32) NOT NULL,
    sku_id           VARCHAR(64) NOT NULL,
    trade_item_id    UUID NOT NULL REFERENCES dim_trade_item(trade_item_id),
    match_method     VARCHAR(32) NOT NULL,
    confidence_score NUMERIC(5,4) NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
    match_status     VARCHAR(32) NOT NULL,
    model_version    VARCHAR(64),
    rule_version     VARCHAR(64) NOT NULL,
    valid_from       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to         TIMESTAMPTZ,
    verified_at      TIMESTAMPTZ,
    UNIQUE (retailer, market_id, sku_id, valid_from),
    CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

-- Enforce at most one active SCD Type 2 mapping per retailer SKU and accelerate point-in-time joins
CREATE UNIQUE INDEX IF NOT EXISTS uq_bridge_sku_trade_item_active
    ON bridge_sku_trade_item (retailer, market_id, sku_id)
    WHERE valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_bridge_sku_trade_item_active_trade_item
    ON bridge_sku_trade_item (trade_item_id)
    WHERE valid_to IS NULL;

CREATE INDEX IF NOT EXISTS idx_bridge_sku_trade_item_scd2_history
    ON bridge_sku_trade_item (retailer, market_id, sku_id, valid_from DESC, valid_to);

CREATE INDEX IF NOT EXISTS idx_bridge_sku_trade_item_active_status
    ON bridge_sku_trade_item (match_status, match_method)
    WHERE valid_to IS NULL;

-- -----------------------------------------------------------------------------
-- 6. Master Catalog: Entity Resolution Human Review & Active Learning Log
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS match_review_log (
    review_id       BIGSERIAL PRIMARY KEY,
    candidate_a     JSONB NOT NULL,
    candidate_b     JSONB NOT NULL,
    suggested_match VARCHAR(32) NOT NULL,
    final_decision  VARCHAR(32) NOT NULL,
    reviewer        VARCHAR(128) NOT NULL,
    decision_reason TEXT,
    model_version   VARCHAR(64),
    reviewed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_match_review_log_reviewed_at
    ON match_review_log (reviewed_at DESC);

CREATE INDEX IF NOT EXISTS idx_match_review_log_final_decision
    ON match_review_log (final_decision, reviewed_at DESC);
