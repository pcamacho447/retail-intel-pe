#!/usr/bin/env python3
"""DuckDB audit script for E1 Bronze Parquet partitions."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import duckdb

PRICE_COLUMNS: tuple[str, ...] = ("list_price", "selling_price", "card_price")
BRONZE_GLOB_PATTERN = "dt=*/retailer=*/part-*.parquet"
METRICS_GLOB_PATTERN = "dt=*/retailer=*/metrics-*.json"
SAMPLE_PRODUCTS_LIMIT = 16
BRONZE_CONTRACT_V2_VERSION = "2.0.0"
VALID_AVAILABILITY_STATUSES: tuple[str, ...] = (
    "in_stock",
    "out_of_stock",
    "listed_unknown",
    "unknown",
)
VALID_AVAILABILITY_SOURCES: tuple[str, ...] = (
    "vtex_offer",
    "tottus_listing_flag",
    "tottus_listing_qty",
    "tottus_listing_presence",
    "tottus_pdp",
    "none",
)
VALID_RUN_STATUSES: tuple[str, ...] = ("success", "truncated")

_GTIN_CLASS_MACRO_SQL = """
CREATE OR REPLACE MACRO gtin_class(e) AS CASE
  WHEN e IS NULL THEN 'missing'
  WHEN NOT regexp_full_match(e, '[0-9]+') THEN 'non_numeric'
  WHEN length(e) NOT IN (8, 12, 13, 14) THEN 'bad_length'
  WHEN length(e) = 13 AND e[1] = '2' THEN 'restricted_circulation'
  WHEN length(e) = 8 AND e[1] IN ('0', '2') THEN 'restricted_circulation'
  ELSE 'global_candidate' END
"""

_EAN_MOD10_PREDICATE_SQL = """(
    CASE
        WHEN ean_clean IS NOT NULL
         AND regexp_full_match(ean_clean, '[0-9]{8}|[0-9]{12}|[0-9]{13}|[0-9]{14}')
         AND TRIM(ean_clean, '0') <> ''
        THEN list_sum([
            TRY_CAST(SUBSTR(LPAD(ean_clean, 14, '0'), i, 1) AS INTEGER)
            * (CASE WHEN i % 2 = 1 THEN 3 ELSE 1 END)
            FOR i IN generate_series(1, 14)
        ]) % 10 = 0
        ELSE FALSE
    END
)"""


def _pct(numerator: int, denominator: int) -> float:
    return round((numerator * 100.0) / denominator, 2) if denominator > 0 else 0.0


def _ratio(numerator: int, denominator: int) -> float:
    return round(numerator / denominator, 4) if denominator > 0 else 0.0


def _round_opt(value: float | None, digits: int = 4) -> float | None:
    return round(float(value), digits) if value is not None else None


def _empty_price_stats(null_count: int = 0) -> dict[str, Any]:
    return {
        "non_null_count": 0,
        "null_count": null_count,
        "min": None,
        "p25": None,
        "p50": None,
        "mean": None,
        "p75": None,
        "max": None,
    }


def _empty_metrics_block() -> dict[str, Any]:
    return {
        "row_count": 0,
        "unique_products": 0,
        "product_observations": 0,
        "unique_skus": 0,
        "sku_to_product_ratio": 0.0,
        "ean_non_null_count": 0,
        "ean_coverage_pct": 0.0,
        "ean_mod10_valid_count": 0,
        "ean_mod10_valid_pct": 0.0,
        "duplicate_flag_count": 0,
        "duplicate_flag_pct": 0.0,
        "distinct_payload_hashes": 0,
        "hash_duplicates_count": 0,
        "hash_duplicates_pct": 0.0,
        "multi_sku_fanout_count": 0,
        "multi_sku_fanout_pct": 0.0,
        "raw_payload_null_count": 0,
        "raw_payload_null_pct": 0.0,
        "card_below_selling_count": 0,
        "card_below_selling_pct": 0.0,
        "card_below_selling_of_card_offers_pct": 0.0,
        "card_program_counts": {},
        "gtin_class_counts": {},
        "ean_source_counts": {},
        "availability_status_counts": {},
        "stock_availability": {
            "in_stock_count": 0,
            "out_of_stock_count": 0,
            "unknown_stock_count": 0,
            "in_stock_pct": 0.0,
        },
        "prices": {col: _empty_price_stats(0) for col in PRICE_COLUMNS},
    }


def _empty_gate_inputs(run_statuses: dict[str, int] | None = None) -> dict[str, Any]:
    return {
        "contract_versions": {},
        "ean_without_source_rows": 0,
        "seed_ean_source_rows": 0,
        "availability_invalid_rows": 0,
        "dirty_or_nogit_version_rows": 0,
        "run_statuses": dict(run_statuses) if run_statuses else {},
    }


def _opt_varchar_expr(present_cols: set[str], col: str, *, trimmed: bool = False) -> str:
    if col not in present_cols:
        return f"CAST(NULL AS VARCHAR) AS {col}"
    if trimmed:
        return f"NULLIF(TRIM(COALESCE(TRY_CAST({col} AS VARCHAR), ''), ' \t\n\r'), '') AS {col}"
    return f"TRY_CAST({col} AS VARCHAR) AS {col}"


def _bronze_cte_sql(present_cols: set[str]) -> str:
    card_program_expr = _opt_varchar_expr(present_cols, "card_program", trimmed=True)
    ean_source_expr = _opt_varchar_expr(present_cols, "ean_source", trimmed=True)
    availability_status_expr = _opt_varchar_expr(present_cols, "availability_status")
    availability_source_expr = _opt_varchar_expr(present_cols, "availability_source")
    extraction_run_id_expr = _opt_varchar_expr(present_cols, "extraction_run_id")
    extractor_version_expr = _opt_varchar_expr(present_cols, "extractor_version")
    source_endpoint_expr = _opt_varchar_expr(present_cols, "source_endpoint")
    contract_version_expr = (
        "COALESCE(NULLIF(TRY_CAST(bronze_contract_version AS VARCHAR), ''), 'legacy') AS bronze_contract_version"
        if "bronze_contract_version" in present_cols
        else "'legacy' AS bronze_contract_version"
    )
    return f"""
    WITH raw_bronze AS (
        SELECT *
        FROM read_parquet(?, hive_partitioning=false, union_by_name=true)
    ),
    bronze AS (
        SELECT
            retailer,
            sku_id,
            product_id,
            raw_name,
            NULLIF(TRIM(COALESCE(TRY_CAST(ean AS VARCHAR), ''), ' \t\n\r'), '') AS ean_clean,
            list_price,
            selling_price,
            card_price,
            {card_program_expr},
            available_qty,
            is_duplicate_payload,
            payload_hash,
            raw_payload,
            {extraction_run_id_expr},
            {extractor_version_expr},
            {source_endpoint_expr},
            {ean_source_expr},
            {availability_status_expr},
            {availability_source_expr},
            {contract_version_expr}
        FROM raw_bronze
    )
    """


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


def _build_audit_sql(present_cols: set[str]) -> str:
    price_selects: list[str] = []
    for col in PRICE_COLUMNS:
        cast_expr = f"TRY_CAST({col} AS DOUBLE)"
        price_selects.extend(
            [
                f"COUNT({cast_expr}) AS {col}_non_null",
                f"MIN({cast_expr}) AS {col}_min",
                f"QUANTILE_CONT({cast_expr}, 0.25) AS {col}_p25",
                f"QUANTILE_CONT({cast_expr}, 0.50) AS {col}_p50",
                f"AVG({cast_expr}) AS {col}_mean",
                f"QUANTILE_CONT({cast_expr}, 0.75) AS {col}_p75",
                f"MAX({cast_expr}) AS {col}_max",
            ]
        )

    price_sql = ",\n        ".join(price_selects)
    valid_statuses_sql = _sql_in_list(VALID_AVAILABILITY_STATUSES)
    valid_sources_sql = _sql_in_list(VALID_AVAILABILITY_SOURCES)
    return f"""
    {_bronze_cte_sql(present_cols)}
    SELECT
        GROUPING(retailer) AS is_total,
        COALESCE(retailer, 'unknown') AS retailer,
        COUNT(*) AS row_count,
        COUNT(DISTINCT struct_pack(r := retailer, pid := product_id)) AS unique_products,
        COUNT(DISTINCT struct_pack(
            r := retailer,
            pid := product_id,
            run_id := extraction_run_id,
            ep := source_endpoint
        )) AS product_observations,
        COUNT(DISTINCT struct_pack(r := retailer, sid := sku_id)) AS unique_skus,
        COUNT(*) FILTER (WHERE ean_clean IS NOT NULL) AS ean_non_null_count,
        COUNT(*) FILTER (WHERE {_EAN_MOD10_PREDICATE_SQL}) AS ean_mod10_valid_count,
        COUNT(*) FILTER (WHERE is_duplicate_payload IS TRUE) AS duplicate_flag_count,
        COUNT(DISTINCT payload_hash) AS distinct_payload_hashes,
        COUNT(*) FILTER (WHERE raw_payload IS NULL) AS raw_payload_null_count,
        COUNT(*) FILTER (
            WHERE TRY_CAST(card_price AS DOUBLE) IS NOT NULL
              AND TRY_CAST(selling_price AS DOUBLE) IS NOT NULL
              AND TRY_CAST(card_price AS DOUBLE) < TRY_CAST(selling_price AS DOUBLE)
        ) AS card_below_selling_count,
        COUNT(*) FILTER (
            WHERE TRY_CAST(card_price AS DOUBLE) IS NOT NULL
              AND TRY_CAST(selling_price AS DOUBLE) IS NOT NULL
        ) AS card_comparable_count,
        COUNT(*) FILTER (WHERE TRY_CAST(available_qty AS BIGINT) > 0) AS in_stock_count,
        COUNT(*) FILTER (
            WHERE TRY_CAST(available_qty AS BIGINT) IS NOT NULL
              AND TRY_CAST(available_qty AS BIGINT) <= 0
        ) AS out_of_stock_count,
        COUNT(*) FILTER (WHERE available_qty IS NULL) AS unknown_stock_count,
        COUNT(*) FILTER (WHERE ean_clean IS NOT NULL AND ean_source IS NULL) AS ean_without_source_rows,
        COUNT(*) FILTER (WHERE LOWER(COALESCE(ean_source, '')) LIKE '%seed%') AS seed_ean_source_rows,
        COUNT(*) FILTER (
            WHERE availability_status IS NULL
               OR availability_status NOT IN ({valid_statuses_sql})
               OR availability_source IS NULL
               OR availability_source NOT IN ({valid_sources_sql})
               OR (availability_source = 'none' AND availability_status <> 'unknown')
        ) AS availability_invalid_rows,
        COUNT(*) FILTER (
            WHERE extractor_version IS NULL
               OR TRIM(extractor_version) = ''
               OR LOWER(extractor_version) LIKE '%-dirty'
               OR LOWER(extractor_version) LIKE '%+nogit'
        ) AS dirty_or_nogit_version_rows,
        {price_sql}
    FROM bronze
    GROUP BY GROUPING SETS ((retailer), ())
    ORDER BY is_total ASC, retailer ASC
    """


def _build_breakdown_sql(
    present_cols: set[str],
    value_expr: str,
    *,
    where_clause: str | None = None,
) -> str:
    where_sql = f"WHERE {where_clause}" if where_clause else ""
    return f"""
    {_bronze_cte_sql(present_cols)}
    SELECT
        GROUPING(retailer) AS is_total,
        COALESCE(retailer, 'unknown') AS retailer,
        ({value_expr}) AS bucket,
        COUNT(*) AS row_count
    FROM bronze
    {where_sql}
    GROUP BY GROUPING SETS ((retailer, bucket), (bucket))
    ORDER BY is_total ASC, retailer ASC, row_count DESC, bucket ASC
    """


def _build_contract_versions_sql(present_cols: set[str]) -> str:
    return f"""
    {_bronze_cte_sql(present_cols)}
    SELECT
        bronze_contract_version AS contract_version,
        COUNT(*) AS row_count
    FROM bronze
    GROUP BY bronze_contract_version
    ORDER BY row_count DESC, contract_version ASC
    """


def _build_sample_products_sql(present_cols: set[str]) -> str:
    return f"""
    {_bronze_cte_sql(present_cols)},
    scored AS (
        SELECT
            COALESCE(retailer, 'unknown') AS retailer,
            sku_id,
            TRY_CAST(raw_name AS VARCHAR) AS raw_name,
            ean_clean AS ean,
            TRY_CAST(list_price AS DOUBLE) AS list_price,
            TRY_CAST(selling_price AS DOUBLE) AS selling_price,
            TRY_CAST(card_price AS DOUBLE) AS card_price,
            card_program,
            TRY_CAST(available_qty AS BIGINT) AS available_qty,
            (TRY_CAST(card_price AS DOUBLE) IS NOT NULL) AS has_card_price,
            {_EAN_MOD10_PREDICATE_SQL} AS has_valid_mod10_ean,
            (ean_clean IS NOT NULL) AS has_non_null_ean,
            (COALESCE(TRY_CAST(available_qty AS BIGINT), 0) > 0) AS in_stock,
            COALESCE(is_duplicate_payload, FALSE) AS is_dup
        FROM bronze
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY COALESCE(retailer, 'unknown'), sku_id
            ORDER BY
                (TRY_CAST(card_price AS DOUBLE) IS NOT NULL) DESC,
                {_EAN_MOD10_PREDICATE_SQL} DESC,
                (ean_clean IS NOT NULL) DESC,
                (COALESCE(TRY_CAST(available_qty AS BIGINT), 0) > 0) DESC,
                COALESCE(is_duplicate_payload, FALSE) ASC
        ) = 1
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY retailer
                ORDER BY
                    has_card_price DESC,
                    has_valid_mod10_ean DESC,
                    has_non_null_ean DESC,
                    in_stock DESC,
                    is_dup ASC,
                    sku_id ASC
            ) AS retailer_rank
        FROM scored
    )
    SELECT
        retailer,
        sku_id,
        raw_name,
        ean,
        list_price,
        selling_price,
        card_price,
        card_program,
        available_qty
    FROM ranked
    ORDER BY
        retailer_rank ASC,
        has_card_price DESC,
        has_valid_mod10_ean DESC,
        retailer ASC,
        sku_id ASC
    LIMIT {SAMPLE_PRODUCTS_LIMIT}
    """


def _row_to_metrics_block(row_map: dict[str, Any]) -> dict[str, Any]:
    row_count = int(row_map["row_count"])
    unique_products = int(row_map["unique_products"])
    product_observations = int(row_map["product_observations"])
    unique_skus = int(row_map["unique_skus"])
    ean_non_null_count = int(row_map["ean_non_null_count"])
    ean_mod10_valid_count = int(row_map["ean_mod10_valid_count"])
    duplicate_flag_count = int(row_map["duplicate_flag_count"])
    distinct_payload_hashes = int(row_map["distinct_payload_hashes"])
    hash_duplicates_count = max(0, product_observations - distinct_payload_hashes)
    multi_sku_fanout_count = max(0, row_count - product_observations)
    raw_payload_null_count = int(row_map["raw_payload_null_count"])
    card_below_selling_count = int(row_map["card_below_selling_count"])
    card_comparable_count = int(row_map["card_comparable_count"])
    in_stock_count = int(row_map["in_stock_count"])
    out_of_stock_count = int(row_map["out_of_stock_count"])
    unknown_stock_count = int(row_map["unknown_stock_count"])

    prices: dict[str, dict[str, Any]] = {}
    for col in PRICE_COLUMNS:
        non_null = int(row_map[f"{col}_non_null"])
        prices[col] = {
            "non_null_count": non_null,
            "null_count": row_count - non_null,
            "min": _round_opt(row_map[f"{col}_min"]),
            "p25": _round_opt(row_map[f"{col}_p25"]),
            "p50": _round_opt(row_map[f"{col}_p50"]),
            "mean": _round_opt(row_map[f"{col}_mean"]),
            "p75": _round_opt(row_map[f"{col}_p75"]),
            "max": _round_opt(row_map[f"{col}_max"]),
        }

    return {
        "row_count": row_count,
        "unique_products": unique_products,
        "product_observations": product_observations,
        "unique_skus": unique_skus,
        "sku_to_product_ratio": _ratio(unique_skus, unique_products),
        "ean_non_null_count": ean_non_null_count,
        "ean_coverage_pct": _pct(ean_non_null_count, row_count),
        "ean_mod10_valid_count": ean_mod10_valid_count,
        "ean_mod10_valid_pct": _pct(ean_mod10_valid_count, row_count),
        "duplicate_flag_count": duplicate_flag_count,
        "duplicate_flag_pct": _pct(duplicate_flag_count, row_count),
        "distinct_payload_hashes": distinct_payload_hashes,
        "hash_duplicates_count": hash_duplicates_count,
        "hash_duplicates_pct": _pct(hash_duplicates_count, product_observations),
        "multi_sku_fanout_count": multi_sku_fanout_count,
        "multi_sku_fanout_pct": _pct(multi_sku_fanout_count, row_count),
        "raw_payload_null_count": raw_payload_null_count,
        "raw_payload_null_pct": _pct(raw_payload_null_count, row_count),
        "card_below_selling_count": card_below_selling_count,
        "card_below_selling_pct": _pct(card_below_selling_count, row_count),
        "card_below_selling_of_card_offers_pct": _pct(
            card_below_selling_count, card_comparable_count
        ),
        "card_program_counts": {},
        "gtin_class_counts": {},
        "ean_source_counts": {},
        "availability_status_counts": {},
        "stock_availability": {
            "in_stock_count": in_stock_count,
            "out_of_stock_count": out_of_stock_count,
            "unknown_stock_count": unknown_stock_count,
            "in_stock_pct": _pct(in_stock_count, row_count),
        },
        "prices": prices,
    }


def _fetch_dicts(con: duckdb.DuckDBPyConnection, sql: str, params: list[Any]) -> list[dict[str, Any]]:
    cursor = con.execute(sql, params)
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, values)) for values in cursor.fetchall()]


def _scan_run_statuses(bronze_dir: Path) -> dict[str, int]:
    if not bronze_dir.exists():
        return {}
    statuses: dict[str, int] = {}
    for metrics_path in sorted(bronze_dir.glob(METRICS_GLOB_PATTERN)):
        try:
            payload = json.loads(metrics_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            status = "invalid"
        else:
            raw_status = payload.get("status") if isinstance(payload, dict) else None
            status = (
                raw_status.strip()
                if isinstance(raw_status, str) and raw_status.strip()
                else "invalid"
            )
        statuses[status] = statuses.get(status, 0) + 1
    return statuses


def _populate_breakdown(
    rows: list[dict[str, Any]],
    total_block: dict[str, Any],
    by_retailer: dict[str, dict[str, Any]],
    target_key: str,
) -> None:
    for item in rows:
        bucket = str(item["bucket"])
        count = int(item["row_count"])
        if int(item["is_total"]) == 1:
            total_block[target_key][bucket] = count
        else:
            retailer_key = str(item["retailer"])
            if retailer_key in by_retailer:
                by_retailer[retailer_key][target_key][bucket] = count


def audit_bronze(bronze_dir: Path) -> dict[str, Any]:
    """Query partitioned Bronze Parquet files with DuckDB and compute audit metrics."""
    resolved_dir = Path(bronze_dir)
    run_statuses = _scan_run_statuses(resolved_dir)
    parquet_files = (
        sorted(resolved_dir.glob(BRONZE_GLOB_PATTERN))
        if resolved_dir.exists()
        else []
    )

    if not parquet_files:
        return {
            "bronze_dir": str(resolved_dir),
            "files_scanned": 0,
            "files": [],
            "has_data": False,
            "total": _empty_metrics_block(),
            "by_retailer": {},
            "sample_products": [],
            "gate_inputs": _empty_gate_inputs(run_statuses),
        }

    file_args = [str(p) for p in parquet_files]
    breakdown_specs: tuple[tuple[str, str, str | None], ...] = (
        (
            "card_program_counts",
            "card_program",
            "card_program IS NOT NULL AND TRY_CAST(card_price AS DOUBLE) IS NOT NULL",
        ),
        ("gtin_class_counts", "gtin_class(ean_clean)", None),
        ("ean_source_counts", "ean_source", "ean_source IS NOT NULL"),
        (
            "availability_status_counts",
            "availability_status",
            "availability_status IS NOT NULL",
        ),
    )

    with duckdb.connect(database=":memory:") as con:
        con.execute(_GTIN_CLASS_MACRO_SQL)
        desc_rows = con.execute(
            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false, union_by_name=true)",
            [file_args],
        ).fetchall()
        present_cols = {str(r[0]) for r in desc_rows}

        rows = _fetch_dicts(con, _build_audit_sql(present_cols), [file_args])
        breakdown_rows = {
            key: _fetch_dicts(
                con,
                _build_breakdown_sql(present_cols, expr, where_clause=where_sql),
                [file_args],
            )
            for key, expr, where_sql in breakdown_specs
        }
        contract_rows = _fetch_dicts(
            con, _build_contract_versions_sql(present_cols), [file_args]
        )
        sample_rows = _fetch_dicts(
            con, _build_sample_products_sql(present_cols), [file_args]
        )

    by_retailer: dict[str, dict[str, Any]] = {}
    total_block = _empty_metrics_block()
    total_row: dict[str, Any] | None = None

    for row in rows:
        metrics = _row_to_metrics_block(row)
        if int(row["is_total"]) == 1:
            total_block = metrics
            total_row = row
        else:
            by_retailer[str(row["retailer"])] = metrics

    for target_key, items in breakdown_rows.items():
        _populate_breakdown(items, total_block, by_retailer, target_key)

    gate_inputs = _empty_gate_inputs(run_statuses)
    gate_inputs["contract_versions"] = {
        str(item["contract_version"]): int(item["row_count"])
        for item in contract_rows
    }
    if total_row is not None:
        gate_inputs["ean_without_source_rows"] = int(total_row["ean_without_source_rows"])
        gate_inputs["seed_ean_source_rows"] = int(total_row["seed_ean_source_rows"])
        gate_inputs["availability_invalid_rows"] = int(total_row["availability_invalid_rows"])
        gate_inputs["dirty_or_nogit_version_rows"] = int(total_row["dirty_or_nogit_version_rows"])

    sample_products = [
        {
            "retailer": str(item["retailer"]),
            "sku_id": str(item["sku_id"]),
            "raw_name": str(item["raw_name"]) if item["raw_name"] is not None else "",
            "ean": str(item["ean"]) if item["ean"] is not None else None,
            "list_price": _round_opt(item["list_price"]),
            "selling_price": _round_opt(item["selling_price"]),
            "card_price": _round_opt(item["card_price"]),
            "card_program": (
                str(item["card_program"]) if item["card_program"] is not None else None
            ),
            "available_qty": (
                int(item["available_qty"]) if item["available_qty"] is not None else None
            ),
        }
        for item in sample_rows
    ]

    return {
        "bronze_dir": str(resolved_dir),
        "files_scanned": len(parquet_files),
        "files": file_args,
        "has_data": total_block["row_count"] > 0,
        "total": total_block,
        "by_retailer": by_retailer,
        "sample_products": sample_products,
        "gate_inputs": gate_inputs,
    }


def evaluate_gate(
    report: dict[str, Any], *, allow_truncated: bool = False
) -> list[str]:
    """Evaluate Bronze Contract v2 gate rules G1..G5 and return violated rule codes."""
    gate_inputs = report.get("gate_inputs", {})
    contract_versions: dict[str, int] = gate_inputs.get("contract_versions", {})
    run_statuses: dict[str, int] = gate_inputs.get("run_statuses", {})

    violations: list[str] = []

    if any(
        version != BRONZE_CONTRACT_V2_VERSION and int(count) > 0
        for version, count in contract_versions.items()
    ):
        violations.append("G1_CONTRACT_VERSION")

    if (
        int(gate_inputs.get("ean_without_source_rows", 0)) > 0
        or int(gate_inputs.get("seed_ean_source_rows", 0)) > 0
    ):
        violations.append("G2_EAN_PROVENANCE")

    if int(gate_inputs.get("availability_invalid_rows", 0)) > 0:
        violations.append("G3_AVAILABILITY_DOMAIN")

    if int(gate_inputs.get("dirty_or_nogit_version_rows", 0)) > 0:
        violations.append("G4_EXTRACTOR_VERSION")

    has_disallowed_status = any(
        status not in VALID_RUN_STATUSES and int(count) > 0
        for status, count in run_statuses.items()
    )
    has_disallowed_truncated = (
        int(run_statuses.get("truncated", 0)) > 0 and not allow_truncated
    )
    if has_disallowed_status or has_disallowed_truncated:
        violations.append("G5_RUN_STATUS")

    return violations


def _fmt_price(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "N/A"


def _fmt_card_offer(card_price: float | None, card_program: str | None) -> str:
    if card_price is None:
        return "N/A"
    if card_program:
        return f"{card_price:.2f} ({card_program})"
    return f"{card_price:.2f}"


def _format_counts(counts: dict[str, int]) -> str:
    if not counts:
        return "_None_"
    return ", ".join(f"`{key}`: {count}" for key, count in counts.items())


def _md_cell(value: str | None, fallback: str = "N/A") -> str:
    if value is None or not str(value).strip():
        return fallback
    return " ".join(str(value).replace("|", "\\|").split())


def format_markdown_report(report: dict[str, Any]) -> str:
    """Render an audit dictionary as a clear Markdown report."""
    lines: list[str] = [
        "## Bronze Layer Audit Report",
        "",
        f"- **Bronze Dir:** `{report['bronze_dir']}`",
        f"- **Parquet Files Scanned:** `{report['files_scanned']}`",
        "",
    ]

    if not report["has_data"]:
        lines.append(
            f"_No Bronze Parquet rows found matching `{BRONZE_GLOB_PATTERN}`._"
        )
        run_statuses = report.get("gate_inputs", {}).get("run_statuses", {})
        if run_statuses:
            lines.append("")
            lines.append(f"- **Run Statuses:** {_format_counts(run_statuses)}")
        return "\n".join(lines)

    lines.extend(
        [
            "### Coverage, Deduplication & Stock Summary",
            "",
            "| Scope | Rows | Products | SKUs | SKU/Prod | EAN no nulo (%) | EAN Mod-10 (%) | "
            "Dup Flag (%) | Hash Dups (%) | 1:N Fanout (%) | Null `raw_payload` (%) | In Stock (%) | Card < Selling (%) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    scopes: list[tuple[str, dict[str, Any]]] = [
        (retailer, metrics) for retailer, metrics in report["by_retailer"].items()
    ]
    scopes.append(("**TOTAL**", report["total"]))

    for label, m in scopes:
        lines.append(
            f"| {label} | {m['row_count']} | {m['unique_products']} | {m['unique_skus']} | "
            f"{m['sku_to_product_ratio']:.2f} | {m['ean_coverage_pct']:.2f}% | "
            f"{m.get('ean_mod10_valid_pct', 0.0):.2f}% | "
            f"{m['duplicate_flag_pct']:.2f}% | {m['hash_duplicates_pct']:.2f}% | "
            f"{m.get('multi_sku_fanout_pct', 0.0):.2f}% | "
            f"{m['raw_payload_null_pct']:.2f}% | {m['stock_availability']['in_stock_pct']:.2f}% | "
            f"{m['card_below_selling_pct']:.2f}% |"
        )

    gate_inputs = report.get("gate_inputs", {})
    lines.extend(
        [
            "",
            "### Contract v2, Provenance & GTIN Summary",
            "",
            f"- **Contract Versions:** {_format_counts(gate_inputs.get('contract_versions', {}))}",
            f"- **Run Statuses:** {_format_counts(gate_inputs.get('run_statuses', {}))}",
            f"- **Gate Violations Counters:** `ean_without_source_rows`: {gate_inputs.get('ean_without_source_rows', 0)}, "
            f"`seed_ean_source_rows`: {gate_inputs.get('seed_ean_source_rows', 0)}, "
            f"`availability_invalid_rows`: {gate_inputs.get('availability_invalid_rows', 0)}, "
            f"`dirty_or_nogit_version_rows`: {gate_inputs.get('dirty_or_nogit_version_rows', 0)}",
            "",
            "| Scope | Product Obs | GTIN Classes | EAN Sources | Availability Status |",
            "| :--- | ---: | :--- | :--- | :--- |",
        ]
    )
    for label, m in scopes:
        lines.append(
            f"| {label} | {m.get('product_observations', 0)} | "
            f"{_format_counts(m.get('gtin_class_counts', {}))} | "
            f"{_format_counts(m.get('ean_source_counts', {}))} | "
            f"{_format_counts(m.get('availability_status_counts', {}))} |"
        )

    lines.extend(["", "### Card Programs Breakdown", ""])
    total_programs = report["total"].get("card_program_counts", {})
    if not total_programs:
        lines.append("_No active card program offers detected._")
    else:
        lines.extend(
            [
                "| Scope | Program | Active Offers |",
                "| :--- | :--- | ---: |",
            ]
        )
        for label, m in scopes:
            prog_counts = m.get("card_program_counts", {})
            if not prog_counts:
                lines.append(f"| {label} | _None_ | 0 |")
            else:
                for prog, count in prog_counts.items():
                    lines.append(f"| {label} | {prog} | {count} |")

    lines.extend(
        [
            "",
            "### Price Distribution (PEN)",
            "",
            "| Scope | Metric | Non-Null | Min | P25 | P50 | Mean | P75 | Max |",
            "| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )

    for label, m in scopes:
        for price_col in PRICE_COLUMNS:
            p = m["prices"][price_col]
            lines.append(
                f"| {label} | `{price_col}` | {p['non_null_count']} | "
                f"{_fmt_price(p['min'])} | {_fmt_price(p['p25'])} | {_fmt_price(p['p50'])} | "
                f"{_fmt_price(p['mean'])} | {_fmt_price(p['p75'])} | {_fmt_price(p['max'])} |"
            )

    samples = report.get("sample_products", [])
    lines.extend(["", "### Sample Multi-Retailer Observations", ""])
    if not samples:
        lines.append("_No sample observations available._")
    else:
        lines.extend(
            [
                "| Retailer | SKU | Producto | P. Lista | P. Venta | P. Tarjeta + Programa | EAN | Stock |",
                "| :--- | :--- | :--- | ---: | ---: | :--- | :--- | ---: |",
            ]
        )
        for item in samples:
            stock_str = (
                str(item["available_qty"])
                if item.get("available_qty") is not None
                else "N/A"
            )
            lines.append(
                f"| {_md_cell(item.get('retailer'))} | {_md_cell(item.get('sku_id'))} | "
                f"{_md_cell(item.get('raw_name'))} | {_fmt_price(item.get('list_price'))} | "
                f"{_fmt_price(item.get('selling_price'))} | "
                f"{_fmt_card_offer(item.get('card_price'), item.get('card_program'))} | "
                f"{_md_cell(item.get('ean'))} | {stock_str} |"
            )

    return "\n".join(lines)


def _write_markdown(path: Path, content: str, append: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if append and path.exists():
        existing = path.read_text(encoding="utf-8")
        separator = "" if (not existing or existing.endswith("\n\n")) else ("\n" if existing.endswith("\n") else "\n\n")
        path.write_text(f"{existing}{separator}{content}\n", encoding="utf-8")
    else:
        path.write_text(f"{content}\n", encoding="utf-8")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Audit E1 Bronze Parquet files using DuckDB."
    )
    parser.add_argument(
        "--bronze-dir",
        type=Path,
        default=Path("data/bronze"),
        help="Root directory of partitioned Bronze Parquet files (default: data/bronze).",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=None,
        help="Optional path to write the audit summary as JSON.",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=None,
        help="Optional path to write the Markdown audit report.",
    )
    parser.add_argument(
        "--append-md",
        action="store_true",
        help="Append Markdown report to --output-md if the file already exists.",
    )
    parser.add_argument(
        "--gate",
        action="store_true",
        help="Evaluate Contract v2 gate rules and exit 1 on violations.",
    )
    parser.add_argument(
        "--allow-truncated",
        action="store_true",
        help="Allow runs with status 'truncated' when evaluating --gate.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    report = audit_bronze(args.bronze_dir)
    md_report = format_markdown_report(report)
    json_summary = json.dumps(report, indent=2, ensure_ascii=False)

    if args.output_json is not None:
        args.output_json.parent.mkdir(parents=True, exist_ok=True)
        args.output_json.write_text(f"{json_summary}\n", encoding="utf-8")

    if args.output_md is not None:
        _write_markdown(args.output_md, md_report, append=args.append_md)

    print(md_report)
    print("\n### JSON Summary\n")
    print(f"```json\n{json_summary}\n```")

    if args.gate:
        violations = evaluate_gate(report, allow_truncated=args.allow_truncated)
        if violations:
            print(
                f"Bronze contract v2 gate FAILED: {', '.join(violations)}",
                file=sys.stderr,
            )
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
