#!/usr/bin/env python3
"""DuckDB audit script for E1 Bronze Parquet partitions."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

PRICE_COLUMNS: tuple[str, ...] = ("list_price", "selling_price", "card_price")
BRONZE_GLOB_PATTERN = "dt=*/retailer=*/part-*.parquet"


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
        "unique_skus": 0,
        "sku_to_product_ratio": 0.0,
        "ean_non_null_count": 0,
        "ean_coverage_pct": 0.0,
        "duplicate_flag_count": 0,
        "duplicate_flag_pct": 0.0,
        "distinct_payload_hashes": 0,
        "hash_duplicates_count": 0,
        "hash_duplicates_pct": 0.0,
        "raw_payload_null_count": 0,
        "raw_payload_null_pct": 0.0,
        "card_below_selling_count": 0,
        "card_below_selling_pct": 0.0,
        "card_below_selling_of_card_offers_pct": 0.0,
        "stock_availability": {
            "in_stock_count": 0,
            "out_of_stock_count": 0,
            "unknown_stock_count": 0,
            "in_stock_pct": 0.0,
        },
        "prices": {col: _empty_price_stats(0) for col in PRICE_COLUMNS},
    }


def _build_audit_sql() -> str:
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
    return f"""
    WITH bronze AS (
        SELECT *
        FROM read_parquet(?, hive_partitioning=false, union_by_name=true)
    )
    SELECT
        GROUPING(retailer) AS is_total,
        COALESCE(retailer, 'unknown') AS retailer,
        COUNT(*) AS row_count,
        COUNT(DISTINCT struct_pack(r := retailer, pid := product_id)) AS unique_products,
        COUNT(DISTINCT struct_pack(r := retailer, sid := sku_id)) AS unique_skus,
        COUNT(*) FILTER (WHERE ean IS NOT NULL AND LENGTH(TRIM(ean)) > 0) AS ean_non_null_count,
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
        {price_sql}
    FROM bronze
    GROUP BY GROUPING SETS ((retailer), ())
    ORDER BY is_total ASC, retailer ASC
    """


def _row_to_metrics_block(row_map: dict[str, Any]) -> dict[str, Any]:
    row_count = int(row_map["row_count"])
    unique_products = int(row_map["unique_products"])
    unique_skus = int(row_map["unique_skus"])
    ean_non_null_count = int(row_map["ean_non_null_count"])
    duplicate_flag_count = int(row_map["duplicate_flag_count"])
    distinct_payload_hashes = int(row_map["distinct_payload_hashes"])
    hash_duplicates_count = row_count - distinct_payload_hashes
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
        "unique_skus": unique_skus,
        "sku_to_product_ratio": _ratio(unique_skus, unique_products),
        "ean_non_null_count": ean_non_null_count,
        "ean_coverage_pct": _pct(ean_non_null_count, row_count),
        "duplicate_flag_count": duplicate_flag_count,
        "duplicate_flag_pct": _pct(duplicate_flag_count, row_count),
        "distinct_payload_hashes": distinct_payload_hashes,
        "hash_duplicates_count": hash_duplicates_count,
        "hash_duplicates_pct": _pct(hash_duplicates_count, row_count),
        "raw_payload_null_count": raw_payload_null_count,
        "raw_payload_null_pct": _pct(raw_payload_null_count, row_count),
        "card_below_selling_count": card_below_selling_count,
        "card_below_selling_pct": _pct(card_below_selling_count, row_count),
        "card_below_selling_of_card_offers_pct": _pct(
            card_below_selling_count, card_comparable_count
        ),
        "stock_availability": {
            "in_stock_count": in_stock_count,
            "out_of_stock_count": out_of_stock_count,
            "unknown_stock_count": unknown_stock_count,
            "in_stock_pct": _pct(in_stock_count, row_count),
        },
        "prices": prices,
    }


def audit_bronze(bronze_dir: Path) -> dict[str, Any]:
    """Query partitioned Bronze Parquet files with DuckDB and compute audit metrics."""
    resolved_dir = Path(bronze_dir)
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
        }

    sql = _build_audit_sql()
    file_args = [str(p) for p in parquet_files]
    with duckdb.connect(database=":memory:") as con:
        cursor = con.execute(sql, [file_args])
        columns = [desc[0] for desc in cursor.description]
        rows = [dict(zip(columns, values)) for values in cursor.fetchall()]

    by_retailer: dict[str, dict[str, Any]] = {}
    total_block = _empty_metrics_block()

    for row in rows:
        metrics = _row_to_metrics_block(row)
        if int(row["is_total"]) == 1:
            total_block = metrics
        else:
            by_retailer[str(row["retailer"])] = metrics

    return {
        "bronze_dir": str(resolved_dir),
        "files_scanned": len(parquet_files),
        "files": file_args,
        "has_data": total_block["row_count"] > 0,
        "total": total_block,
        "by_retailer": by_retailer,
    }


def _fmt_price(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "N/A"


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
        return "\n".join(lines)

    lines.extend(
        [
            "### Coverage, Deduplication & Stock Summary",
            "",
            "| Scope | Rows | Products | SKUs | SKU/Prod | EAN Valid (%) | "
            "Dup Flag (%) | Hash Dups (%) | Null `raw_payload` (%) | In Stock (%) | Card < Selling (%) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
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
            f"{m['duplicate_flag_pct']:.2f}% | {m['hash_duplicates_pct']:.2f}% | "
            f"{m['raw_payload_null_pct']:.2f}% | {m['stock_availability']['in_stock_pct']:.2f}% | "
            f"{m['card_below_selling_pct']:.2f}% |"
        )

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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
