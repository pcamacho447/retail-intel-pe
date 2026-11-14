from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from scripts.inspect_bronze import audit_bronze, format_markdown_report, main  # noqa: E402

BRONZE_ARROW_SCHEMA = pa.schema(
    [
        pa.field("retailer", pa.string(), nullable=False),
        pa.field("market_id", pa.string(), nullable=False),
        pa.field("store_id", pa.string(), nullable=True),
        pa.field("sku_id", pa.string(), nullable=False),
        pa.field("product_id", pa.string(), nullable=False),
        pa.field("ean", pa.string(), nullable=True),
        pa.field("seller_id", pa.string(), nullable=True),
        pa.field("raw_name", pa.string(), nullable=False),
        pa.field("brand", pa.string(), nullable=True),
        pa.field("category_path", pa.string(), nullable=True),
        pa.field("raw_description", pa.string(), nullable=True),
        pa.field("product_url", pa.string(), nullable=True),
        pa.field("image_url", pa.string(), nullable=True),
        pa.field("list_price", pa.float64(), nullable=True),
        pa.field("selling_price", pa.float64(), nullable=True),
        pa.field("card_price", pa.float64(), nullable=True),
        pa.field("card_program", pa.string(), nullable=True),
        pa.field("available_qty", pa.int64(), nullable=True),
        pa.field("currency", pa.string(), nullable=False),
        pa.field("sales_channel", pa.string(), nullable=False),
        pa.field("postal_code", pa.string(), nullable=True),
        pa.field("scraped_at", pa.timestamp("ms", tz="UTC"), nullable=False),
        pa.field("extraction_run_id", pa.string(), nullable=False),
        pa.field("extractor_version", pa.string(), nullable=False),
        pa.field("source_endpoint", pa.string(), nullable=False),
        pa.field("http_status", pa.uint16(), nullable=False),
        pa.field("payload_hash", pa.string(), nullable=False),
        pa.field("is_duplicate_payload", pa.bool_(), nullable=False),
        pa.field("raw_payload", pa.string(), nullable=True),
    ]
)


def _make_row(
    *,
    retailer: str,
    sku_id: str,
    product_id: str,
    ean: str | None,
    list_price: float | None,
    selling_price: float | None,
    card_price: float | None,
    available_qty: int | None,
    payload_hash: str,
    is_duplicate_payload: bool,
    raw_payload: str | None,
    run_id: str,
    card_program: str | None = None,
    raw_name: str | None = None,
) -> dict:
    return {
        "retailer": retailer,
        "market_id": "PE",
        "store_id": "lima-centro",
        "sku_id": sku_id,
        "product_id": product_id,
        "ean": ean,
        "seller_id": "1",
        "raw_name": raw_name if raw_name is not None else f"Producto {sku_id}",
        "brand": "Gloria",
        "category_path": "Lácteos/Leches",
        "raw_description": "Descripción de prueba",
        "product_url": f"https://example.com/{retailer}/{sku_id}",
        "image_url": f"https://example.com/img/{sku_id}.jpg",
        "list_price": list_price,
        "selling_price": selling_price,
        "card_price": card_price,
        "card_program": card_program,
        "available_qty": available_qty,
        "currency": "PEN",
        "sales_channel": "ecommerce",
        "postal_code": "15001",
        "scraped_at": datetime(2026, 11, 13, 18, 0, 0, tzinfo=timezone.utc),
        "extraction_run_id": run_id,
        "extractor_version": "0.1.0",
        "source_endpoint": f"https://api.example.com/{retailer}/catalog",
        "http_status": 200,
        "payload_hash": payload_hash,
        "is_duplicate_payload": is_duplicate_payload,
        "raw_payload": raw_payload,
    }


def _write_bronze_partition(
    bronze_dir: Path,
    *,
    dt: str,
    retailer: str,
    filename: str,
    rows: list[dict],
) -> Path:
    part_dir = bronze_dir / f"dt={dt}" / f"retailer={retailer}"
    part_dir.mkdir(parents=True, exist_ok=True)
    out_path = part_dir / filename
    table = pa.Table.from_pylist(rows, schema=BRONZE_ARROW_SCHEMA)
    pq.write_table(table, out_path, compression="zstd", compression_level=3)
    return out_path


@pytest.fixture
def sample_bronze_dir(tmp_path: Path) -> Path:
    pv_rows = [
        _make_row(
            retailer="plazavea",
            sku_id="pv-s1",
            product_id="pv-p1",
            ean="7750182001001",
            list_price=12.0,
            selling_price=10.0,
            card_price=9.0,
            available_qty=5,
            payload_hash="h_pv_1",
            is_duplicate_payload=False,
            raw_payload='{"id":"pv-p1"}',
            run_id="run1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-s2",
            product_id="pv-p1",
            ean="7750182001002",
            list_price=24.0,
            selling_price=20.0,
            card_price=20.0,
            available_qty=0,
            payload_hash="h_pv_1",
            is_duplicate_payload=False,
            raw_payload='{"id":"pv-p1"}',
            run_id="run1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-s3",
            product_id="pv-p2",
            ean="",
            list_price=30.0,
            selling_price=30.0,
            card_price=25.0,
            available_qty=12,
            payload_hash="h_pv_2",
            is_duplicate_payload=True,
            raw_payload=None,
            run_id="run1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-s4",
            product_id="pv-p2",
            ean="   ",
            list_price=40.0,
            selling_price=40.0,
            card_price=None,
            available_qty=None,
            payload_hash="h_pv_2",
            is_duplicate_payload=True,
            raw_payload=None,
            run_id="run1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-s5",
            product_id="pv-p3",
            ean=None,
            list_price=50.0,
            selling_price=50.0,
            card_price=None,
            available_qty=3,
            payload_hash="h_pv_3",
            is_duplicate_payload=False,
            raw_payload='{"id":"pv-p3"}',
            run_id="run1",
        ),
    ]

    metro_rows = [
        _make_row(
            retailer="metro",
            sku_id="mt-s1",
            product_id="mt-p1",
            ean="7750182002001",
            list_price=10.0,
            selling_price=8.0,
            card_price=None,
            available_qty=10,
            payload_hash="h_mt_1",
            is_duplicate_payload=False,
            raw_payload='{"id":"mt-p1"}',
            run_id="run2",
        ),
        _make_row(
            retailer="metro",
            sku_id="mt-s2",
            product_id="mt-p1",
            ean="7750182002002",
            list_price=20.0,
            selling_price=16.0,
            card_price=None,
            available_qty=0,
            payload_hash="h_mt_1",
            is_duplicate_payload=False,
            raw_payload='{"id":"mt-p1"}',
            run_id="run2",
        ),
        _make_row(
            retailer="metro",
            sku_id="mt-s3",
            product_id="mt-p1",
            ean=None,
            list_price=30.0,
            selling_price=24.0,
            card_price=None,
            available_qty=2,
            payload_hash="h_mt_1",
            is_duplicate_payload=False,
            raw_payload='{"id":"mt-p1"}',
            run_id="run2",
        ),
        _make_row(
            retailer="metro",
            sku_id="mt-s4",
            product_id="mt-p2",
            ean="7750182002004",
            list_price=None,
            selling_price=32.0,
            card_price=None,
            available_qty=4,
            payload_hash="h_mt_2",
            is_duplicate_payload=True,
            raw_payload=None,
            run_id="run2",
        ),
    ]

    _write_bronze_partition(
        tmp_path,
        dt="2026-11-13",
        retailer="plazavea",
        filename="part-run1.parquet",
        rows=pv_rows,
    )
    _write_bronze_partition(
        tmp_path,
        dt="2026-11-13",
        retailer="metro",
        filename="part-run2.parquet",
        rows=metro_rows,
    )
    return tmp_path


def test_counts_unique_products_vs_skus_and_ratio(sample_bronze_dir: Path) -> None:
    report = audit_bronze(sample_bronze_dir)

    assert report["has_data"] is True
    assert report["files_scanned"] == 2

    pv = report["by_retailer"]["plazavea"]
    assert pv["row_count"] == 5
    assert pv["unique_products"] == 3
    assert pv["unique_skus"] == 5
    assert pv["sku_to_product_ratio"] == pytest.approx(5 / 3, rel=1e-3)

    metro = report["by_retailer"]["metro"]
    assert metro["row_count"] == 4
    assert metro["unique_products"] == 2
    assert metro["unique_skus"] == 4
    assert metro["sku_to_product_ratio"] == pytest.approx(2.0, rel=1e-4)

    total = report["total"]
    assert total["row_count"] == 9
    assert total["unique_products"] == 5
    assert total["unique_skus"] == 9
    assert total["sku_to_product_ratio"] == pytest.approx(9 / 5, rel=1e-4)


def test_ean_coverage_ignores_null_empty_and_whitespace(sample_bronze_dir: Path) -> None:
    report = audit_bronze(sample_bronze_dir)

    pv = report["by_retailer"]["plazavea"]
    assert pv["ean_non_null_count"] == 2
    assert pv["ean_coverage_pct"] == pytest.approx(40.0, rel=1e-4)

    metro = report["by_retailer"]["metro"]
    assert metro["ean_non_null_count"] == 3
    assert metro["ean_coverage_pct"] == pytest.approx(75.0, rel=1e-4)

    total = report["total"]
    assert total["ean_non_null_count"] == 5
    assert total["ean_coverage_pct"] == pytest.approx(55.56, rel=1e-3)


def test_duplicate_rates_flag_hash_and_null_raw_payload(sample_bronze_dir: Path) -> None:
    report = audit_bronze(sample_bronze_dir)

    pv = report["by_retailer"]["plazavea"]
    assert pv["duplicate_flag_count"] == 2
    assert pv["duplicate_flag_pct"] == pytest.approx(40.0, rel=1e-4)
    assert pv["distinct_payload_hashes"] == 3
    assert pv["hash_duplicates_count"] == 2
    assert pv["hash_duplicates_pct"] == pytest.approx(40.0, rel=1e-4)
    assert pv["raw_payload_null_count"] == 2
    assert pv["raw_payload_null_pct"] == pytest.approx(40.0, rel=1e-4)

    metro = report["by_retailer"]["metro"]
    assert metro["duplicate_flag_count"] == 1
    assert metro["duplicate_flag_pct"] == pytest.approx(25.0, rel=1e-4)
    assert metro["distinct_payload_hashes"] == 2
    assert metro["hash_duplicates_count"] == 2
    assert metro["hash_duplicates_pct"] == pytest.approx(50.0, rel=1e-4)
    assert metro["raw_payload_null_count"] == 1
    assert metro["raw_payload_null_pct"] == pytest.approx(25.0, rel=1e-4)

    total = report["total"]
    assert total["duplicate_flag_count"] == 3
    assert total["duplicate_flag_pct"] == pytest.approx(33.33, rel=1e-3)
    assert total["distinct_payload_hashes"] == 5
    assert total["hash_duplicates_count"] == 4
    assert total["hash_duplicates_pct"] == pytest.approx(44.44, rel=1e-3)
    assert total["raw_payload_null_count"] == 3
    assert total["raw_payload_null_pct"] == pytest.approx(33.33, rel=1e-3)


def test_price_distributions_and_all_null_card_price(sample_bronze_dir: Path) -> None:
    report = audit_bronze(sample_bronze_dir)

    pv = report["by_retailer"]["plazavea"]
    pv_selling = pv["prices"]["selling_price"]
    assert pv_selling["non_null_count"] == 5
    assert pv_selling["null_count"] == 0
    assert pv_selling["min"] == pytest.approx(10.0)
    assert pv_selling["p25"] == pytest.approx(20.0)
    assert pv_selling["p50"] == pytest.approx(30.0)
    assert pv_selling["mean"] == pytest.approx(30.0)
    assert pv_selling["p75"] == pytest.approx(40.0)
    assert pv_selling["max"] == pytest.approx(50.0)

    pv_card = pv["prices"]["card_price"]
    assert pv_card["non_null_count"] == 3
    assert pv_card["null_count"] == 2
    assert pv_card["min"] == pytest.approx(9.0)
    assert pv_card["p25"] == pytest.approx(14.5)
    assert pv_card["p50"] == pytest.approx(20.0)
    assert pv_card["mean"] == pytest.approx(18.0)
    assert pv_card["p75"] == pytest.approx(22.5)
    assert pv_card["max"] == pytest.approx(25.0)
    assert pv["card_below_selling_count"] == 2
    assert pv["card_below_selling_pct"] == pytest.approx(40.0)

    metro = report["by_retailer"]["metro"]
    metro_list = metro["prices"]["list_price"]
    assert metro_list["non_null_count"] == 3
    assert metro_list["null_count"] == 1
    assert metro_list["min"] == pytest.approx(10.0)
    assert metro_list["p50"] == pytest.approx(20.0)
    assert metro_list["mean"] == pytest.approx(20.0)
    assert metro_list["max"] == pytest.approx(30.0)

    metro_card = metro["prices"]["card_price"]
    assert metro_card["non_null_count"] == 0
    assert metro_card["null_count"] == 4
    assert metro_card["min"] is None
    assert metro_card["p25"] is None
    assert metro_card["p50"] is None
    assert metro_card["mean"] is None
    assert metro_card["p75"] is None
    assert metro_card["max"] is None
    assert metro["card_below_selling_count"] == 0
    assert metro["card_below_selling_pct"] == pytest.approx(0.0)
    assert metro["card_below_selling_of_card_offers_pct"] == pytest.approx(0.0)


def test_stock_availability_metrics(sample_bronze_dir: Path) -> None:
    report = audit_bronze(sample_bronze_dir)
    pv_stock = report["by_retailer"]["plazavea"]["stock_availability"]
    assert pv_stock["in_stock_count"] == 3
    assert pv_stock["out_of_stock_count"] == 1
    assert pv_stock["unknown_stock_count"] == 1
    assert pv_stock["in_stock_pct"] == pytest.approx(60.0)


def test_empty_and_missing_bronze_dir_handled_safely(tmp_path: Path) -> None:
    empty_dir = tmp_path / "empty_bronze"
    empty_dir.mkdir()

    empty_report = audit_bronze(empty_dir)
    assert empty_report["has_data"] is False
    assert empty_report["files_scanned"] == 0
    assert empty_report["by_retailer"] == {}
    assert empty_report["total"]["row_count"] == 0
    assert empty_report["total"]["unique_products"] == 0
    assert empty_report["total"]["unique_skus"] == 0
    assert empty_report["total"]["sku_to_product_ratio"] == 0.0
    assert empty_report["total"]["ean_coverage_pct"] == 0.0
    assert empty_report["total"]["duplicate_flag_pct"] == 0.0
    assert empty_report["total"]["card_below_selling_pct"] == 0.0
    assert empty_report["total"]["prices"]["card_price"]["min"] is None

    missing_report = audit_bronze(tmp_path / "does_not_exist")
    assert missing_report["has_data"] is False
    assert missing_report["files_scanned"] == 0


def test_cli_outputs_json_and_markdown_with_append_mode(
    sample_bronze_dir: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    out_json = tmp_path / "audit.json"
    out_md = tmp_path / "audit.md"

    exit_code = main(
        [
            "--bronze-dir",
            str(sample_bronze_dir),
            "--output-json",
            str(out_json),
            "--output-md",
            str(out_md),
        ]
    )
    assert exit_code == 0

    captured = capsys.readouterr()
    assert "plazavea" in captured.out
    assert "metro" in captured.out
    assert '"files_scanned": 2' in captured.out

    payload = json.loads(out_json.read_text(encoding="utf-8"))
    assert payload["total"]["row_count"] == 9
    assert "plazavea" in payload["by_retailer"]

    md_text = out_md.read_text(encoding="utf-8")
    assert "plazavea" in md_text
    assert "metro" in md_text

    out_md.write_text("# Preexisting Header\n\n", encoding="utf-8")
    exit_code_append = main(
        [
            "--bronze-dir",
            str(sample_bronze_dir),
            "--output-md",
            str(out_md),
            "--append-md",
        ]
    )
    assert exit_code_append == 0
    appended_text = out_md.read_text(encoding="utf-8")
    assert appended_text.startswith("# Preexisting Header\n\n")
    assert "plazavea" in appended_text


def test_zero_row_parquet_and_pa_null_card_price(tmp_path: Path) -> None:
    empty_part_dir = tmp_path / "zero_rows"
    _write_bronze_partition(
        empty_part_dir,
        dt="2026-11-13",
        retailer="plazavea",
        filename="part-empty.parquet",
        rows=[],
    )
    zero_report = audit_bronze(empty_part_dir)
    assert zero_report["files_scanned"] == 1
    assert zero_report["has_data"] is False
    assert zero_report["total"]["row_count"] == 0
    assert zero_report["by_retailer"] == {}

    null_typed_dir = tmp_path / "null_typed"
    part_dir = null_typed_dir / "dt=2026-11-13" / "retailer=tottus"
    part_dir.mkdir(parents=True)
    row = _make_row(
        retailer="tottus",
        sku_id="tt-1",
        product_id="p-shared",
        ean="7750182009999",
        list_price=15.0,
        selling_price=12.0,
        card_price=None,
        available_qty=5,
        payload_hash="h_tt_1",
        is_duplicate_payload=False,
        raw_payload='{"ok":true}',
        run_id="run3",
    )
    table = pa.Table.from_pylist([row], schema=BRONZE_ARROW_SCHEMA)
    card_idx = table.schema.get_field_index("card_price")
    table = table.set_column(card_idx, "card_price", pa.nulls(1, type=pa.null()))
    pq.write_table(table, part_dir / "part-run3.parquet", compression="zstd")

    pv_row = _make_row(
        retailer="plazavea",
        sku_id="tt-1",
        product_id="p-shared",
        ean="7750182009999",
        list_price=16.0,
        selling_price=14.0,
        card_price=11.0,
        available_qty=2,
        payload_hash="h_pv_x",
        is_duplicate_payload=False,
        raw_payload='{"ok":true}',
        run_id="run4",
    )
    _write_bronze_partition(
        null_typed_dir,
        dt="2026-11-13",
        retailer="plazavea",
        filename="part-run4.parquet",
        rows=[pv_row],
    )

    report = audit_bronze(null_typed_dir)
    assert report["total"]["row_count"] == 2
    assert report["total"]["unique_products"] == 2
    assert report["total"]["unique_skus"] == 2
    assert report["by_retailer"]["tottus"]["prices"]["card_price"]["min"] is None
    assert report["by_retailer"]["plazavea"]["prices"]["card_price"]["min"] == pytest.approx(11.0)


def test_ean_mod10_validation_filters_invalid_and_garbage_eans(tmp_path: Path) -> None:
    rows = [
        # Valid GTIN-8, UPC-12, EAN-13, GTIN-14
        _make_row(
            retailer="tottus",
            sku_id="s1",
            product_id="p1",
            ean=" 20181512 ",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h1",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s2",
            product_id="p2",
            ean="012345678905",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h2",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s3",
            product_id="p3",
            ean="7751271034142",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h3",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s4",
            product_id="p4",
            ean="17750151008669",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h4",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        # Invalid lengths, bad Mod-10 checksums, all-zeros, literal "null", whitespace
        _make_row(
            retailer="tottus",
            sku_id="s5",
            product_id="p5",
            ean="151258566",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h5",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s6",
            product_id="p6",
            ean="20181521",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h6",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s7",
            product_id="p7",
            ean="null",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h7",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s8",
            product_id="p8",
            ean="00000000",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h8",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s9",
            product_id="p9",
            ean="7751271034143",
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h9",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="tottus",
            sku_id="s10",
            product_id="p10",
            ean=None,
            list_price=10.0,
            selling_price=9.0,
            card_price=None,
            available_qty=5,
            payload_hash="h10",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
    ]
    _write_bronze_partition(
        tmp_path,
        dt="2026-11-13",
        retailer="tottus",
        filename="part-r1.parquet",
        rows=rows,
    )

    report = audit_bronze(tmp_path)
    tottus = report["by_retailer"]["tottus"]
    assert tottus["ean_non_null_count"] == 9
    assert tottus["ean_coverage_pct"] == pytest.approx(90.0)
    assert tottus["ean_mod10_valid_count"] == 4
    assert tottus["ean_mod10_valid_pct"] == pytest.approx(40.0)
    assert report["total"]["ean_mod10_valid_count"] == 4
    assert report["total"]["ean_mod10_valid_pct"] == pytest.approx(40.0)


def test_multi_sku_fanout_separates_1_to_n_from_duplicate_flags(tmp_path: Path) -> None:
    rows = [
        # 1 product expanded to 3 distinct SKUs (none are duplicate payloads)
        _make_row(
            retailer="metro",
            sku_id="sku-1",
            product_id="prod-a",
            ean="7751271034142",
            list_price=15.0,
            selling_price=12.0,
            card_price=None,
            available_qty=5,
            payload_hash="hash-a",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="metro",
            sku_id="sku-2",
            product_id="prod-a",
            ean="20181512",
            list_price=16.0,
            selling_price=13.0,
            card_price=None,
            available_qty=5,
            payload_hash="hash-a",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="metro",
            sku_id="sku-3",
            product_id="prod-a",
            ean="012345678905",
            list_price=17.0,
            selling_price=14.0,
            card_price=None,
            available_qty=5,
            payload_hash="hash-a",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        # 1 product flagged as true duplicate payload
        _make_row(
            retailer="metro",
            sku_id="sku-4",
            product_id="prod-b",
            ean=None,
            list_price=20.0,
            selling_price=18.0,
            card_price=None,
            available_qty=2,
            payload_hash="hash-b",
            is_duplicate_payload=True,
            raw_payload=None,
            run_id="r1",
        ),
    ]
    _write_bronze_partition(
        tmp_path,
        dt="2026-11-13",
        retailer="metro",
        filename="part-r1.parquet",
        rows=rows,
    )

    report = audit_bronze(tmp_path)
    metro = report["by_retailer"]["metro"]
    assert metro["row_count"] == 4
    assert metro["unique_products"] == 2
    assert metro["unique_skus"] == 4
    assert metro["duplicate_flag_count"] == 1
    assert metro["duplicate_flag_pct"] == pytest.approx(25.0)
    assert metro["multi_sku_fanout_count"] == 2
    assert metro["multi_sku_fanout_pct"] == pytest.approx(50.0)
    assert report["total"]["multi_sku_fanout_count"] == 2
    assert report["total"]["multi_sku_fanout_pct"] == pytest.approx(50.0)

    md = format_markdown_report(report)
    assert "EAN Mod-10 (%)" in md
    assert "1:N Fanout (%)" in md


def test_card_program_counts_sample_products_and_markdown_sections(tmp_path: Path) -> None:
    pv_rows = [
        _make_row(
            retailer="plazavea",
            sku_id="pv-1",
            product_id="pv-p1",
            ean="7751271034142",
            list_price=12.0,
            selling_price=10.5,
            card_price=9.0,
            card_program="Oh!",
            raw_name="Leche Gloria 390g",
            available_qty=10,
            payload_hash="hp1",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-2",
            product_id="pv-p2",
            ean="151258566",  # invalid EAN
            list_price=15.0,
            selling_price=14.0,
            card_price=12.0,
            card_program="Oh!",
            raw_name="Aceite Primor 1L",
            available_qty=4,
            payload_hash="hp2",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
        _make_row(
            retailer="plazavea",
            sku_id="pv-3",
            product_id="pv-p3",
            ean="20181512",
            list_price=8.0,
            selling_price=7.5,
            card_price=None,
            card_program="Oh!",  # card_price is None -> must NOT count!
            raw_name="Arroz Costeño 750g",
            available_qty=8,
            payload_hash="hp3",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r1",
        ),
    ]
    metro_rows = [
        _make_row(
            retailer="metro",
            sku_id="mt-1",
            product_id="mt-p1",
            ean="012345678905",
            list_price=22.0,
            selling_price=19.9,
            card_price=17.9,
            card_program="Cencosud",
            raw_name="Café Altomayo",
            available_qty=3,
            payload_hash="hm1",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r2",
        ),
    ]
    tottus_rows = [
        _make_row(
            retailer="tottus",
            sku_id="tt-1",
            product_id="tt-p1",
            ean="17750151008669",
            list_price=14.0,
            selling_price=13.0,
            card_price=11.5,
            card_program="CMR",
            raw_name="Avena Quaker 500g",
            available_qty=7,
            payload_hash="ht1",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="r3",
        ),
    ]

    _write_bronze_partition(tmp_path, dt="2026-11-13", retailer="plazavea", filename="part-r1.parquet", rows=pv_rows)
    _write_bronze_partition(tmp_path, dt="2026-11-13", retailer="metro", filename="part-r2.parquet", rows=metro_rows)
    _write_bronze_partition(tmp_path, dt="2026-11-13", retailer="tottus", filename="part-r3.parquet", rows=tottus_rows)

    report = audit_bronze(tmp_path)
    assert report["by_retailer"]["plazavea"]["card_program_counts"] == {"Oh!": 2}
    assert report["by_retailer"]["metro"]["card_program_counts"] == {"Cencosud": 1}
    assert report["by_retailer"]["tottus"]["card_program_counts"] == {"CMR": 1}
    assert report["total"]["card_program_counts"] == {"Oh!": 2, "Cencosud": 1, "CMR": 1}

    samples = report["sample_products"]
    assert 1 <= len(samples) <= 16
    assert len(samples) == 5
    required_keys = {
        "retailer",
        "sku_id",
        "raw_name",
        "ean",
        "list_price",
        "selling_price",
        "card_price",
        "card_program",
        "available_qty",
    }
    assert all(required_keys.issubset(item.keys()) for item in samples)
    # Prioritizes SKUs with card_price IS NOT NULL and valid Mod-10 EAN before pv-3 (card_price=None)
    assert samples[-1]["sku_id"] == "pv-3"

    md = format_markdown_report(report)
    assert "### Card Programs Breakdown" in md
    assert "### Sample Multi-Retailer Observations" in md
    assert "P. Tarjeta + Programa" in md
    assert "9.00 (Oh!)" in md
    assert "17.90 (Cencosud)" in md
    assert "11.50 (CMR)" in md


def test_legacy_28_column_parquet_and_empty_metrics_include_new_fields(tmp_path: Path) -> None:
    empty_report = audit_bronze(tmp_path / "missing")
    assert empty_report["sample_products"] == []
    assert empty_report["total"]["ean_mod10_valid_count"] == 0
    assert empty_report["total"]["ean_mod10_valid_pct"] == 0.0
    assert empty_report["total"]["multi_sku_fanout_count"] == 0
    assert empty_report["total"]["multi_sku_fanout_pct"] == 0.0
    assert empty_report["total"]["card_program_counts"] == {}

    legacy_dir = tmp_path / "legacy_28col"
    part_dir = legacy_dir / "dt=2026-11-13" / "retailer=plazavea"
    part_dir.mkdir(parents=True)
    legacy_schema = pa.schema(
        [field for field in BRONZE_ARROW_SCHEMA if field.name != "card_program"]
    )
    row = _make_row(
        retailer="plazavea",
        sku_id="leg-1",
        product_id="leg-p1",
        ean="7751271034142",
        list_price=10.0,
        selling_price=9.0,
        card_price=8.0,
        available_qty=1,
        payload_hash="hl1",
        is_duplicate_payload=False,
        raw_payload="{}",
        run_id="r-leg",
    )
    row.pop("card_program")
    table = pa.Table.from_pylist([row], schema=legacy_schema)
    assert table.num_columns == 28
    pq.write_table(table, part_dir / "part-legacy.parquet", compression="zstd")

    report = audit_bronze(legacy_dir)
    assert report["total"]["row_count"] == 1
    assert report["total"]["ean_mod10_valid_count"] == 1
    assert report["total"]["card_program_counts"] == {}
    assert report["sample_products"][0]["card_program"] is None
    md = format_markdown_report(report)
    assert "### Sample Multi-Retailer Observations" in md


def test_schema_has_29_columns_and_samples_cap_at_16_across_mixed_schemas(tmp_path: Path) -> None:
    assert len(BRONZE_ARROW_SCHEMA) == 29
    field_names = BRONZE_ARROW_SCHEMA.names
    assert field_names[field_names.index("card_price") + 1] == "card_program"

    # Write 5 rows for each of 3 retailers using 29-column schema + 5 rows for 4th using 28-column schema
    for retailer, program in [("plazavea", "Oh!"), ("metro", "Cencosud"), ("wong", "Wong Prime")]:
        rows = [
            _make_row(
                retailer=retailer,
                sku_id=f"{retailer}-{i}",
                product_id=f"{retailer}-p-{i}",
                ean="7751271034142",
                list_price=10.0 + i,
                selling_price=9.0 + i,
                card_price=8.0 + i,
                card_program=program,
                available_qty=5,
                payload_hash=f"h-{retailer}-{i}",
                is_duplicate_payload=False,
                raw_payload="{}",
                run_id=f"run-{retailer}",
            )
            for i in range(5)
        ]
        _write_bronze_partition(
            tmp_path,
            dt="2026-11-14",
            retailer=retailer,
            filename=f"part-{retailer}.parquet",
            rows=rows,
        )

    legacy_schema = pa.schema(
        [field for field in BRONZE_ARROW_SCHEMA if field.name != "card_program"]
    )
    tottus_rows = []
    for i in range(5):
        r = _make_row(
            retailer="tottus",
            sku_id=f"tottus-{i}",
            product_id=f"tottus-p-{i}",
            ean="20181512",
            list_price=12.0 + i,
            selling_price=11.0 + i,
            card_price=10.0 + i,
            available_qty=3,
            payload_hash=f"h-tottus-{i}",
            is_duplicate_payload=False,
            raw_payload="{}",
            run_id="run-tottus",
        )
        r.pop("card_program")
        tottus_rows.append(r)
    tottus_dir = tmp_path / "dt=2026-11-14" / "retailer=tottus"
    tottus_dir.mkdir(parents=True)
    pq.write_table(
        pa.Table.from_pylist(tottus_rows, schema=legacy_schema),
        tottus_dir / "part-tottus.parquet",
        compression="zstd",
    )

    report = audit_bronze(tmp_path)
    assert report["total"]["row_count"] == 20
    assert report["total"]["card_program_counts"] == {
        "Cencosud": 5,
        "Oh!": 5,
        "Wong Prime": 5,
    }
    assert len(report["sample_products"]) == 16
    sampled_retailers = {item["retailer"] for item in report["sample_products"]}
    assert sampled_retailers == {"plazavea", "metro", "wong", "tottus"}
