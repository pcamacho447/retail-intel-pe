# E1 `rust-ingestor` Hardening & Code Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. First save this plan document to `docs/superpowers/plans/2026-11-14-e1-rust-ingestor-hardening.md`, then execute Tasks 1–4 sequentially.

**Goal:** Remediate the prioritized findings from the `rust-engineer` code review on `rust-ingestor` (daily checkpoint expiration and `page_size`/category validation, contextual `postal_code`/`store_id` URL tracking, concurrent merge-safe deduplication without zero-SKU pollution, resilient item skipping and Peruvian price parsing in `falabella.rs`/`vtex.rs`, `Retry-After` HTTP support, `Cargo.lock` tracking, and zero warnings under `clippy::pedantic`).

**Architecture:** Incremental TDD hardening across `checkpoint.rs`, `dedup.rs`, `runner.rs`, `adapters/{falabella,vtex}.rs`, `http.rs`, and `main.rs` within the `rust-ingestor` Cargo workspace. Existing public CLI flags, Arrow schema (28 columns), and file paths (`data/bronze/checkpoints/<retailer>_<category>.json` and `data/bronze/blobs/seen_hashes.json`) remain backward-compatible so all 42 existing Rust tests and 8 Python DuckDB audit tests continue to pass alongside new regression tests.

**Tech Stack:** Rust 2021 (`rust-version = "1.82"`, `tokio`, `reqwest`, `governor`, `serde`, `serde_json`, `arrow`, `parquet`, `chrono`, `sha2`, `uuid`, `tracing`, `clap`), `cargo test`, `cargo clippy -- -W clippy::pedantic`.

**Spec:** `docs/architecture-e1.md` and `docs/qa-report-e1.md`

## Global Constraints
- Maintain backward-compatible JSON serialization for `CheckpointState` (`#[serde(default)]` on new optional fields) and keep `CheckpointStore::path_for` producing `<retailer>_<sanitized_category>.json`.
- Maintain the 28-column Bronze Arrow schema in `parquet_writer.rs` unchanged.
- Every task must follow TDD: write the failing unit test first, run `cargo test` to observe the failure (or compile error), implement the minimal code, and verify `cargo test` passes.
- Zero `unsafe` code; zero warnings under `cargo clippy --all-targets -- -W clippy::pedantic -D warnings`.

## Review Focus
- Stale checkpoint from a previous UTC day (`updated_at.date_naive() != run_date`) with `completed = true`: `--resume` must NOT skip the category today; it must reset to `offset = 0` and fetch fresh prices.
- Checkpoint resumed with a different `--page-size` or a colliding category slug (`"leche/entera"` vs `"leche entera"`): `CheckpointState::is_valid_for_run` must reject the checkpoint if `cp.category != category` or `cp.page_size` mismatches `config.page_size`.
- Concurrent `PayloadDeduplicator` instances writing to `blobs/seen_hashes.json`: `persist()` must re-read any existing file on disk and union its hashes with `self.seen` before atomic replacement so neither run loses hashes, and `runner.rs` must not mark hashes for products with 0 SKUs.
- Peruvian price strings in `FalabellaListingAdapter` (`"S/. 4.50"`, `"S/ 4,50"`, `"S/ 1,299.90"`, `"S/ 1.299,90"`, `"S/ 4.50 - S/ 6.00"`): `parse_price_value` must return `Some(4.50)`, `Some(4.50)`, `Some(1299.90)`, `Some(1299.90)`, and `Some(4.50)` respectively instead of `None` or `450.0`.
- Single malformed product missing `productId` inside a valid VTEX or Falabella page array: parser must log a `tracing::warn!` and skip only that item instead of aborting the whole page.

---

### Step 0: Persist Plan Document
- [ ] Write this plan to `docs/superpowers/plans/2026-11-14-e1-rust-ingestor-hardening.md`.

---

### Task 1: Daily-Scoped Checkpoints with `page_size` & Category Collision Protection (Fixes A1, MIN-01, MIN-09)

**Files:**
- Modify: `rust-ingestor/crates/ingestor/src/checkpoint.rs:9-84`
- Modify: `rust-ingestor/crates/ingestor/src/runner.rs:62-125`
- Test: `rust-ingestor/crates/ingestor/src/checkpoint.rs` and `rust-ingestor/crates/ingestor/src/runner.rs`

**Interfaces:**
- Produces in `checkpoint.rs`:
  - `CheckpointState` with `#[serde(default)] pub page_size: Option<usize>`
  - `CheckpointState::is_valid_for_run(&self, category: &str, run_date: chrono::NaiveDate, page_size: usize) -> bool`
- Consumes in `runner.rs`:
  - Calls `cp.is_valid_for_run(category, run_date, config.page_size)` when `config.resume` is `true`; if `false`, logs `tracing::info!` and starts from `offset = 0, pages_completed_total = 0`.
  - Populates `page_size: Some(config.page_size)` when creating `CheckpointState`.

- [x] **Step 1: Write failing unit tests in `checkpoint.rs` and `runner.rs`**
  - In `checkpoint.rs::tests`, add `checkpoint_is_valid_for_run_checks_date_category_and_page_size`:
    - Verify a checkpoint with `updated_at` today, matching `category: "leche/entera"`, and `page_size: Some(20)` returns `true` for `("leche/entera", today, 20)`.
    - Verify it returns `false` when `run_date` is tomorrow (`today.succ_opt().unwrap()`), when `category` is `"leche entera"` (slug collision), or when `page_size` is `50`.
    - Verify legacy JSON without `"page_size"` deserializes with `page_size: None` and returns `true` for any `page_size` on the same date and category.
  - In `runner.rs::tests`, add `run_ingest_resume_ignores_stale_previous_day_checkpoint`:
    - Save a `completed: true` checkpoint with `updated_at: Utc::now() - chrono::Duration::days(1)`.
    - Run `run_ingest` with `resume: true`.
    - Assert `metrics.pages_fetched == 1` and `metrics.products_recovered == 1` (not skipped).

- [x] **Step 2: Run tests to verify failure (RED)**
  - Run: `cd rust-ingestor && cargo test checkpoint_is_valid_for_run`
  - Expected: FAIL to compile (`page_size` and `is_valid_for_run` not defined).

- [x] **Step 3: Implement `page_size` and `is_valid_for_run` in `checkpoint.rs` and wire into `runner.rs`**
  - Update existing `CheckpointState` struct literals in `checkpoint.rs` and `runner.rs` to include `page_size: Some(20)` (or `Some(config.page_size)`).
  - Implement `CheckpointState::is_valid_for_run(&self, category: &str, run_date: NaiveDate, page_size: usize) -> bool`:
    ```rust
    pub fn is_valid_for_run(&self, category: &str, run_date: NaiveDate, page_size: usize) -> bool {
        self.category == category
            && self.updated_at.date_naive() == run_date
            && self.page_size.is_none_or(|ps| ps == page_size)
    }
    ```
  - In `runner.rs:62-82`, wrap the `if cp.completed` / resume logic inside `if cp.is_valid_for_run(category, run_date, config.page_size) { ... } else { tracing::info!(... "Ignoring stale or mismatched checkpoint"); }`.

- [x] **Step 4: Run tests to verify pass (GREEN)**
  - Run: `cd rust-ingestor && cargo test`
  - Expected: PASS (all existing + 2 new tests).

---

### Task 2: Merge-Safe Deduplication, Zero-SKU Guard & Contextual Endpoint Tracking (Fixes A2, Dedup Race & Zero-SKU Hash Pollution)

**Files:**
- Modify: `rust-ingestor/crates/ingestor/src/dedup.rs:10-67`
- Modify: `rust-ingestor/crates/ingestor/src/runner.rs:98-108`
- Modify: `rust-ingestor/crates/ingestor/src/adapters/vtex.rs:249-265, 345-365`
- Modify: `rust-ingestor/crates/ingestor/src/adapters/falabella.rs:259-305`
- Test: `rust-ingestor/crates/ingestor/src/dedup.rs`, `rust-ingestor/crates/ingestor/src/runner.rs`

**Interfaces:**
- Produces in `dedup.rs`:
  - `PayloadDeduplicator::persist(&mut self) -> anyhow::Result<()>` (or `&self`) that reads any existing `seen_hashes.json` on disk before writing and unions those hashes with `self.seen`, then writes sorted hashes via `crate::atomic_write`.
- Consumes in `runner.rs`:
  - In the product loop (`runner.rs:98-107`), if `product.skus.is_empty()`, skip `dedup.check_and_mark` so products with 0 SKUs do not pollute `seen_hashes.json` without writing a Parquet row.
- Produces in `vtex.rs` and `falabella.rs`:
  - In `fetch_page`, if `ctx.postal_code` is `Some(ref pc)`, append `&_postalCode=<encoded>` (VTEX) or `&postalCode=<encoded>` (Falabella) to the request URL when not already present, and if `ctx.store_id` is `Some(ref sid)`, append `&_storeId=<encoded>` / `&storeId=<encoded>`, so `source_endpoint` and the HTTP request reflect the geographic/store context. Keep `build_page_url` unchanged when called without `ctx`.

- [x] **Step 1: Write failing unit tests in `dedup.rs` and `runner.rs`**
  - In `dedup.rs::tests`, add `deduplicator_persist_merges_concurrent_writes_without_data_loss`:
    - Open `dedup_a` and `dedup_b` on the same `TempDir` simultaneously.
    - Mark `"hash_a"` in `dedup_a` and persist.
    - Mark `"hash_b"` in `dedup_b` and persist.
    - Open `dedup_c` and assert `dedup_c.len() == 2` and both `"hash_a"` and `"hash_b"` are present.
  - In `runner.rs::tests`, add `run_ingest_does_not_mark_hash_when_product_has_zero_skus`:
    - Serve a VTEX product with `"items": []`.
    - Run `run_ingest` and assert `PayloadDeduplicator::open(dir.path()).unwrap().len() == 0`.

- [x] **Step 2: Run tests to verify failure (RED)**
  - Run: `cd rust-ingestor && cargo test deduplicator_persist_merges_concurrent_writes`
  - Expected: FAIL (`dedup_c.len()` is 1 instead of 2).

- [x] **Step 3: Implement merge-on-persist in `dedup.rs`, zero-SKU check in `runner.rs`, and contextual query params in `vtex.rs`/`falabella.rs`**
  - In `dedup.rs::persist(&self)`: if `self.index_path.exists()`, read and parse `SeenHashesFile` and union with `self.seen` before sorting and calling `crate::atomic_write`.
  - In `runner.rs:98-107`:
    ```rust
    for product in &batch.products {
        products_recovered += 1;
        if product.skus.is_empty() {
            continue;
        }
        let is_dup = dedup.check_and_mark(&product.payload_hash);
        ...
    ```
  - In `vtex.rs::fetch_page` and `falabella.rs::fetch_page`: append optional `postalCode`/`storeId` query params from `ctx` to `url` before calling `self.http.get_text(&url)`.

- [x] **Step 4: Run tests to verify pass (GREEN)**
  - Run: `cd rust-ingestor && cargo test`
  - Expected: PASS.

---

### Task 3: Resilient Item Skipping & Peruvian Price Parser in `falabella.rs` and `vtex.rs`

**Files:**
- Modify: `rust-ingestor/crates/ingestor/src/adapters/falabella.rs:73-102, 154-166`
- Modify: `rust-ingestor/crates/ingestor/src/adapters/vtex.rs:212-215, 285-296`
- Test: `rust-ingestor/crates/ingestor/src/adapters/falabella.rs` and `rust-ingestor/crates/ingestor/src/adapters/vtex.rs`

**Interfaces:**
- Produces in `falabella.rs`:
  - `parse_price_value(raw: &Value) -> Option<f64>` supporting `"S/. 4.50"`, `"S/ 4,50"`, `"S/ 1,299.90"`, `"S/ 1.299,90"`, and `"S/ 4.50 - S/ 6.00"`.
  - `parse_results_from_root` skips items missing `productId` with `tracing::warn!` instead of aborting the whole page.
- Produces in `vtex.rs`:
  - `parse_page_payload` skips items missing `productId` with `tracing::warn!` instead of aborting the whole page.

- [x] **Step 1: Write failing unit tests in `falabella.rs` and `vtex.rs`**
  - In `falabella.rs::tests`, add `falabella_parse_price_handles_peruvian_currency_prefixes_commas_and_ranges`:
    - Test `"S/. 4.50"` -> `Some(4.50)`
    - Test `"S/ 4,50"` -> `Some(4.50)`
    - Test `"S/ 1,299.90"` -> `Some(1299.90)`
    - Test `"S/ 1.299,90"` -> `Some(1299.90)`
    - Test `"S/ 4.50 - S/ 6.00"` -> `Some(4.50)`
  - In `falabella.rs::tests` and `vtex.rs::tests`, add tests verifying that a page array containing 1 item without `productId` and 1 valid item returns the 1 valid `Product` instead of `Err`.

- [x] **Step 2: Run tests to verify failure (RED)**
  - Run: `cd rust-ingestor && cargo test falabella_parse_price_handles_peruvian`
  - Expected: FAIL (`"S/. 4.50"` returns `None`, `"S/ 4,50"` returns `Some(450.0)`).

- [x] **Step 3: Implement robust price parser and item skipping in `falabella.rs` and `vtex.rs`**
  - In `falabella.rs::parse_price_value`:
    - Take the first segment before `'-'` if present.
    - Find the first ASCII digit and slice from there, keeping only ASCII digits, `'.'`, and `','`.
    - Normalize thousands vs. decimal separators:
      - If both `'.'` and `','` exist: whichever appears last is the decimal separator (replace the other with `""`, and replace the decimal separator with `'.'`).
      - If only `','` exists: replace `','` with `'.'`.
      - Parse as `f64` and return `Some(parsed)` if `parsed.is_finite() && parsed > 0.0`.
  - In `falabella.rs:158-163` and `vtex.rs:289-294`: replace `.ok_or_else(|| ...)?` on `productId` with `let Some(product_id) = value_as_string(...) else { tracing::warn!(...); continue; };`.

- [x] **Step 4: Run tests to verify pass (GREEN)**
  - Run: `cd rust-ingestor && cargo test`
  - Expected: PASS.

---

### Task 4: HTTP `Retry-After` + Jitter, `Cargo.lock` / `rust-version`, and Zero `clippy::pedantic` Warnings

**Files:**
- Modify: `rust-ingestor/crates/ingestor/src/http.rs:123-186`
- Modify: `rust-ingestor/crates/ingestor/Cargo.toml:1-6`
- Modify: `.gitignore:1-5`
- Modify: `rust-ingestor/crates/ingestor/src/{lib,main,adapter,checkpoint,dedup,domain,metrics,runner}.rs`, `src/adapters/{mod,vtex,falabella}.rs`, `src/storage/parquet_writer.rs` (for `clippy::pedantic` cleanups)

**Interfaces:**
- Produces in `http.rs`:
  - Parses `Retry-After` header (integer seconds, capped at 30s) on `429`/`503` responses and sleeps `backoff_ms.max(retry_after_ms)`.
- Produces in `.gitignore` and `Cargo.toml`:
  - `rust-version = "1.82"` in `rust-ingestor/crates/ingestor/Cargo.toml`.
  - Removes `Cargo.lock` from `.gitignore` so `rust-ingestor/Cargo.lock` is tracked.
- Produces across crate:
  - Clean `cargo clippy --all-targets -- -W clippy::pedantic -D warnings` exit 0.

- [x] **Step 1: Write failing unit test in `http.rs` for `Retry-After` header**
  - Add `http_client_honors_retry_after_header_on_429` in `http.rs::tests` using a mock server returning `HTTP/1.1 429 Too Many Requests\r\nRetry-After: 1\r\n...` with `base_backoff_ms = 5` and asserting elapsed time is `>= 900ms`.

- [x] **Step 2: Run test to verify failure (RED)**
  - Run: `cd rust-ingestor && cargo test http_client_honors_retry_after_header_on_429`
  - Expected: FAIL (sleeps only 5ms instead of ~1000ms).

- [x] **Step 3: Implement `Retry-After` in `http.rs`, update `.gitignore` & `Cargo.toml`, and resolve `clippy::pedantic` warnings**
  - Extract `Retry-After` header in `http.rs` before consuming `resp.text()`, parse seconds (`u64`), clamp to `<= 30`, convert to ms, and pass to `sleep_backoff`.
  - Add `rust-version = "1.82"` to `rust-ingestor/crates/ingestor/Cargo.toml` and remove `Cargo.lock` from `.gitignore`.
  - Fix `uninlined_format_args`, `trivially_copy_pass_by_ref`, `doc_markdown`, `must_use_candidate`, `cast_possible_truncation` / `cast_precision_loss`, and add targeted `#[allow(clippy::missing_errors_doc, clippy::missing_panics_doc, clippy::too_many_lines)]` where appropriate.

- [x] **Step 4: Run full verification suite (GREEN)**
  - Run:
    - `cd rust-ingestor && cargo fmt --check`
    - `cd rust-ingestor && cargo clippy --all-targets -- -W clippy::pedantic -D warnings`
    - `cd rust-ingestor && cargo test`
    - `/home/balonzifer/projects/tracker_retail_prices/.venv/bin/pytest -v`
  - Expected: 0 formatting diffs, 0 clippy pedantic warnings, 100% tests passing.

---

### Re-plan if:
- Any existing test in `runner.rs`, `checkpoint.rs`, `vtex.rs`, `falabella.rs`, or `tests/test_inspect_bronze.py` breaks due to a signature or path change.
- `rustc` version in the environment is older than `1.82` when `rust-version = "1.82"` is set.
- A dependency conflict arises when generating `rust-ingestor/Cargo.lock`.
