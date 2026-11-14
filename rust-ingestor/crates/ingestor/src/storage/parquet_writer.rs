use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    BooleanArray, Float64Array, Int64Array, StringArray, TimestampMillisecondArray, UInt16Array,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::domain::BronzeSkuRow;

#[must_use]
pub fn bronze_arrow_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("retailer", DataType::Utf8, false),
        Field::new("market_id", DataType::Utf8, false),
        Field::new("store_id", DataType::Utf8, true),
        Field::new("sku_id", DataType::Utf8, false),
        Field::new("product_id", DataType::Utf8, false),
        Field::new("ean", DataType::Utf8, true),
        Field::new("seller_id", DataType::Utf8, true),
        Field::new("raw_name", DataType::Utf8, false),
        Field::new("brand", DataType::Utf8, true),
        Field::new("category_path", DataType::Utf8, true),
        Field::new("raw_description", DataType::Utf8, true),
        Field::new("product_url", DataType::Utf8, true),
        Field::new("image_url", DataType::Utf8, true),
        Field::new("list_price", DataType::Float64, true),
        Field::new("selling_price", DataType::Float64, true),
        Field::new("card_price", DataType::Float64, true),
        Field::new("card_program", DataType::Utf8, true),
        Field::new("available_qty", DataType::Int64, true),
        Field::new("currency", DataType::Utf8, false),
        Field::new("sales_channel", DataType::Utf8, false),
        Field::new("postal_code", DataType::Utf8, true),
        Field::new(
            "scraped_at",
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ),
        Field::new("extraction_run_id", DataType::Utf8, false),
        Field::new("extractor_version", DataType::Utf8, false),
        Field::new("source_endpoint", DataType::Utf8, false),
        Field::new("http_status", DataType::UInt16, false),
        Field::new("payload_hash", DataType::Utf8, false),
        Field::new("is_duplicate_payload", DataType::Boolean, false),
        Field::new("raw_payload", DataType::Utf8, true),
    ]))
}

#[must_use]
pub fn parquet_output_path(
    output_dir: &Path,
    dt: NaiveDate,
    retailer: &str,
    run_id: Uuid,
) -> PathBuf {
    output_dir
        .join(format!("dt={}", dt.format("%Y-%m-%d")))
        .join(format!("retailer={retailer}"))
        .join(format!("part-{run_id}.parquet"))
}

fn build_record_batch(schema: Arc<Schema>, rows: &[BronzeSkuRow]) -> anyhow::Result<RecordBatch> {
    let retailer = StringArray::from_iter_values(rows.iter().map(|r| r.retailer.as_str()));
    let market_id = StringArray::from_iter_values(rows.iter().map(|r| r.market_id.as_str()));
    let store_id = StringArray::from_iter(rows.iter().map(|r| r.store_id.as_deref()));
    let sku_id = StringArray::from_iter_values(rows.iter().map(|r| r.sku_id.as_str()));
    let product_id = StringArray::from_iter_values(rows.iter().map(|r| r.product_id.as_str()));
    let ean = StringArray::from_iter(rows.iter().map(|r| r.ean.as_deref()));
    let seller_id = StringArray::from_iter(rows.iter().map(|r| r.seller_id.as_deref()));
    let raw_name = StringArray::from_iter_values(rows.iter().map(|r| r.raw_name.as_str()));
    let brand = StringArray::from_iter(rows.iter().map(|r| r.brand.as_deref()));
    let category_path = StringArray::from_iter(rows.iter().map(|r| r.category_path.as_deref()));
    let raw_description = StringArray::from_iter(rows.iter().map(|r| r.raw_description.as_deref()));
    let product_url = StringArray::from_iter(rows.iter().map(|r| r.product_url.as_deref()));
    let image_url = StringArray::from_iter(rows.iter().map(|r| r.image_url.as_deref()));
    let list_price = Float64Array::from_iter(rows.iter().map(|r| r.list_price));
    let selling_price = Float64Array::from_iter(rows.iter().map(|r| r.selling_price));
    let card_price = Float64Array::from_iter(rows.iter().map(|r| r.card_price));
    let card_program = StringArray::from_iter(rows.iter().map(|r| r.card_program.as_deref()));
    let available_qty = Int64Array::from_iter(rows.iter().map(|r| r.available_qty));
    let currency = StringArray::from_iter_values(rows.iter().map(|r| r.currency.as_str()));
    let sales_channel =
        StringArray::from_iter_values(rows.iter().map(|r| r.sales_channel.as_str()));
    let postal_code = StringArray::from_iter(rows.iter().map(|r| r.postal_code.as_deref()));
    let scraped_at = TimestampMillisecondArray::from_iter_values(
        rows.iter().map(|r| r.scraped_at.timestamp_millis()),
    )
    .with_timezone("UTC");
    let extraction_run_id =
        StringArray::from_iter_values(rows.iter().map(|r| r.extraction_run_id.as_str()));
    let extractor_version =
        StringArray::from_iter_values(rows.iter().map(|r| r.extractor_version.as_str()));
    let source_endpoint =
        StringArray::from_iter_values(rows.iter().map(|r| r.source_endpoint.as_str()));
    let http_status = UInt16Array::from_iter_values(rows.iter().map(|r| r.http_status));
    let payload_hash = StringArray::from_iter_values(rows.iter().map(|r| r.payload_hash.as_str()));
    let is_duplicate_payload =
        BooleanArray::from_iter(rows.iter().map(|r| Some(r.is_duplicate_payload)));
    let raw_payload = StringArray::from_iter(rows.iter().map(|r| r.raw_payload.as_deref()));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(retailer),
            Arc::new(market_id),
            Arc::new(store_id),
            Arc::new(sku_id),
            Arc::new(product_id),
            Arc::new(ean),
            Arc::new(seller_id),
            Arc::new(raw_name),
            Arc::new(brand),
            Arc::new(category_path),
            Arc::new(raw_description),
            Arc::new(product_url),
            Arc::new(image_url),
            Arc::new(list_price),
            Arc::new(selling_price),
            Arc::new(card_price),
            Arc::new(card_program),
            Arc::new(available_qty),
            Arc::new(currency),
            Arc::new(sales_channel),
            Arc::new(postal_code),
            Arc::new(scraped_at),
            Arc::new(extraction_run_id),
            Arc::new(extractor_version),
            Arc::new(source_endpoint),
            Arc::new(http_status),
            Arc::new(payload_hash),
            Arc::new(is_duplicate_payload),
            Arc::new(raw_payload),
        ],
    )
    .context("failed to build Arrow RecordBatch for BronzeSkuRow")
}

pub fn write_bronze_parquet(
    output_dir: &Path,
    dt: NaiveDate,
    retailer: &str,
    run_id: Uuid,
    rows: &[BronzeSkuRow],
) -> anyhow::Result<PathBuf> {
    let out_path = parquet_output_path(output_dir, dt, retailer, run_id);
    let parent = out_path
        .parent()
        .context("Parquet output path must have a parent directory")?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create Bronze partition dir {}", parent.display()))?;

    let schema = bronze_arrow_schema();
    let batch = build_record_batch(schema.clone(), rows)?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();

    let tmp_path = parent.join(format!(".part-{run_id}.parquet.tmp"));
    let write_res = (|| -> anyhow::Result<()> {
        let file = File::create(&tmp_path)
            .with_context(|| format!("failed to create Parquet file {}", tmp_path.display()))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
        if batch.num_rows() > 0 {
            writer.write(&batch)?;
        }
        let inner = writer.into_inner()?;
        inner.sync_all()?;
        Ok(())
    })();

    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    fs::rename(&tmp_path, &out_path).with_context(|| {
        let _ = fs::remove_file(&tmp_path);
        format!("failed to finalize Parquet file {}", out_path.display())
    })?;

    Ok(out_path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Array, BooleanArray, Float64Array, Int64Array, RecordBatchReader, StringArray,
        TimestampMillisecondArray, UInt16Array,
    };
    use arrow::datatypes::{DataType, TimeUnit};
    use chrono::{TimeZone, Utc};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use parquet::file::reader::{FileReader, SerializedFileReader};
    use std::fs::File;
    use tempfile::TempDir;

    fn sample_rows(run_id: Uuid) -> Vec<BronzeSkuRow> {
        let ts = Utc.with_ymd_and_hms(2026, 11, 13, 20, 15, 30).unwrap();
        vec![
            BronzeSkuRow {
                retailer: "plazavea".to_string(),
                market_id: "PE".to_string(),
                store_id: Some("lima-01".to_string()),
                sku_id: "1001".to_string(),
                product_id: "20402646".to_string(),
                ean: Some("7750243012345".to_string()),
                seller_id: Some("1".to_string()),
                raw_name: "Leche Gloria Entera 390g".to_string(),
                brand: Some("Gloria".to_string()),
                category_path: Some("/Lacteos/Leches/".to_string()),
                raw_description: Some("Lata 390g".to_string()),
                product_url: Some("https://www.plazavea.com.pe/leche/p".to_string()),
                image_url: Some("https://img.example.com/1001.jpg".to_string()),
                list_price: Some(5.00),
                selling_price: Some(4.20),
                card_price: Some(3.99),
                card_program: Some("Oh!".to_string()),
                available_qty: Some(42),
                currency: "PEN".to_string(),
                sales_channel: "ecommerce".to_string(),
                postal_code: Some("150101".to_string()),
                scraped_at: ts,
                extraction_run_id: run_id.to_string(),
                extractor_version: "vtex-catalog-v1.0.0".to_string(),
                source_endpoint: "https://www.plazavea.com.pe/api/test".to_string(),
                http_status: 206,
                payload_hash: "hash_new".to_string(),
                is_duplicate_payload: false,
                raw_payload: Some(r#"{"productId":"20402646"}"#.to_string()),
            },
            BronzeSkuRow {
                retailer: "plazavea".to_string(),
                market_id: "PE".to_string(),
                store_id: None,
                sku_id: "1002".to_string(),
                product_id: "20402647".to_string(),
                ean: None,
                seller_id: None,
                raw_name: "Leche Gloria Deslactosada 390g".to_string(),
                brand: None,
                category_path: None,
                raw_description: None,
                product_url: None,
                image_url: None,
                list_price: None,
                selling_price: None,
                card_price: None,
                card_program: None,
                available_qty: None,
                currency: "PEN".to_string(),
                sales_channel: "ecommerce".to_string(),
                postal_code: None,
                scraped_at: ts,
                extraction_run_id: run_id.to_string(),
                extractor_version: "vtex-catalog-v1.0.0".to_string(),
                source_endpoint: "https://www.plazavea.com.pe/api/test".to_string(),
                http_status: 206,
                payload_hash: "hash_dup".to_string(),
                is_duplicate_payload: true,
                raw_payload: None,
            },
        ]
    }

    #[test]
    fn parquet_output_path_follows_hive_partition_layout() {
        let dir = TempDir::new().unwrap();
        let dt = NaiveDate::from_ymd_opt(2026, 11, 13).unwrap();
        let run_id = Uuid::new_v4();
        let path = parquet_output_path(dir.path(), dt, "plazavea", run_id);
        let expected = dir
            .path()
            .join("dt=2026-11-13")
            .join("retailer=plazavea")
            .join(format!("part-{run_id}.parquet"));
        assert_eq!(path, expected);
    }

    #[test]
    fn bronze_arrow_schema_has_exact_29_columns_types_and_nullability() {
        let schema = bronze_arrow_schema();
        assert_eq!(schema.fields().len(), 29);

        let expected = [
            ("retailer", DataType::Utf8, false),
            ("market_id", DataType::Utf8, false),
            ("store_id", DataType::Utf8, true),
            ("sku_id", DataType::Utf8, false),
            ("product_id", DataType::Utf8, false),
            ("ean", DataType::Utf8, true),
            ("seller_id", DataType::Utf8, true),
            ("raw_name", DataType::Utf8, false),
            ("brand", DataType::Utf8, true),
            ("category_path", DataType::Utf8, true),
            ("raw_description", DataType::Utf8, true),
            ("product_url", DataType::Utf8, true),
            ("image_url", DataType::Utf8, true),
            ("list_price", DataType::Float64, true),
            ("selling_price", DataType::Float64, true),
            ("card_price", DataType::Float64, true),
            ("card_program", DataType::Utf8, true),
            ("available_qty", DataType::Int64, true),
            ("currency", DataType::Utf8, false),
            ("sales_channel", DataType::Utf8, false),
            ("postal_code", DataType::Utf8, true),
            (
                "scraped_at",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            ("extraction_run_id", DataType::Utf8, false),
            ("extractor_version", DataType::Utf8, false),
            ("source_endpoint", DataType::Utf8, false),
            ("http_status", DataType::UInt16, false),
            ("payload_hash", DataType::Utf8, false),
            ("is_duplicate_payload", DataType::Boolean, false),
            ("raw_payload", DataType::Utf8, true),
        ];

        for (i, (name, dtype, nullable)) in expected.iter().enumerate() {
            let field = schema.field(i);
            assert_eq!(field.name(), *name, "field {i} name mismatch");
            assert_eq!(field.data_type(), dtype, "field {name} type mismatch");
            assert_eq!(
                field.is_nullable(),
                *nullable,
                "field {name} nullability mismatch"
            );
        }
    }

    #[test]
    fn write_bronze_parquet_creates_zstd_file_and_reads_back_rows_and_nulls() {
        let dir = TempDir::new().unwrap();
        let dt = NaiveDate::from_ymd_opt(2026, 11, 13).unwrap();
        let run_id = Uuid::new_v4();
        let rows = sample_rows(run_id);

        let path = write_bronze_parquet(dir.path(), dt, "plazavea", run_id, &rows).unwrap();
        assert!(path.exists());

        // Verify ZSTD compression in Parquet row group metadata
        let file = File::open(&path).unwrap();
        let parquet_reader = SerializedFileReader::new(file).unwrap();
        let metadata = parquet_reader.metadata();
        assert_eq!(metadata.num_row_groups(), 1);
        let col0_compression = metadata.row_group(0).column(0).compression();
        assert!(
            matches!(col0_compression, Compression::ZSTD(_)),
            "expected ZSTD compression, got {col0_compression:?}"
        );

        // Read back RecordBatch via Arrow reader and verify values + nulls
        let file2 = File::open(&path).unwrap();
        let mut arrow_reader = ParquetRecordBatchReaderBuilder::try_new(file2)
            .unwrap()
            .build()
            .unwrap();
        let batch = arrow_reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 29);

        let sku_col = batch
            .column_by_name("sku_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(sku_col.value(0), "1001");
        assert_eq!(sku_col.value(1), "1002");

        let ean_col = batch
            .column_by_name("ean")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ean_col.value(0), "7750243012345");
        assert!(ean_col.is_null(1));

        let selling_col = batch
            .column_by_name("selling_price")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(selling_col.value(0), 4.20);
        assert!(selling_col.is_null(1));

        let card_prog_col = batch
            .column_by_name("card_program")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(card_prog_col.value(0), "Oh!");
        assert!(card_prog_col.is_null(1));

        let qty_col = batch
            .column_by_name("available_qty")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(qty_col.value(0), 42);
        assert!(qty_col.is_null(1));

        let ts_col = batch
            .column_by_name("scraped_at")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts_col.value(0), rows[0].scraped_at.timestamp_millis());

        let status_col = batch
            .column_by_name("http_status")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt16Array>()
            .unwrap();
        assert_eq!(status_col.value(0), 206);

        let dup_col = batch
            .column_by_name("is_duplicate_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(!dup_col.value(0));
        assert!(dup_col.value(1));

        let raw_payload_col = batch
            .column_by_name("raw_payload")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(raw_payload_col.value(0), r#"{"productId":"20402646"}"#);
        assert!(raw_payload_col.is_null(1));
    }

    #[test]
    fn write_bronze_parquet_handles_empty_rows() {
        let dir = TempDir::new().unwrap();
        let dt = NaiveDate::from_ymd_opt(2026, 11, 13).unwrap();
        let run_id = Uuid::new_v4();

        let path = write_bronze_parquet(dir.path(), dt, "tottus", run_id, &[]).unwrap();
        assert!(path.exists());

        let file = File::open(&path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        assert_eq!(reader.schema().fields().len(), 29);
        let total_rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total_rows, 0);
    }
}
