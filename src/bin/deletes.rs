use std::sync::Arc;

use arrow::array::record_batch;
use iceberg::{
    arrow::{arrow_schema_to_schema, schema_to_arrow_schema},
    spec::DataFileFormat,
    transaction::Transaction,
    writer::{
        base_writer::{
            data_file_writer::DataFileWriterBuilder,
            equality_delete_writer::{EqualityDeleteFileWriterBuilder, EqualityDeleteWriterConfig},
        },
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            ParquetWriterBuilder,
        },
        IcebergWriter, IcebergWriterBuilder,
    },
};
use iceberg_playground::common::init;

#[tokio::main]
async fn main() {
    let (catalog, table) = init().await;

    // Common items
    let file_io = table.file_io().to_owned();
    let loc_gen = DefaultLocationGenerator::new(table.metadata().to_owned()).unwrap();
    let file_gen = DefaultFileNameGenerator::new("PRE".to_string(), None, DataFileFormat::Parquet);

    // APPEND
    let append_schema = table.metadata().current_schema().to_owned();
    let append_record_batch = record_batch!(
        ("name", Utf8, ["A", "B", "C", "A", "E"]),
        (
            "type",
            Utf8,
            ["small", "medium", "medium", "small", "large"]
        ),
        ("count", Int32, [2, 15, 10, 20, 20])
    )
    .expect("Failed to create record batch");

    let mut writer = DataFileWriterBuilder::new(
        ParquetWriterBuilder::new(
            Default::default(),
            append_schema,
            file_io.clone(),
            loc_gen.clone(),
            file_gen.clone(),
        ),
        None,
    )
    .build()
    .await
    .unwrap();
    let _ = writer.write(append_record_batch).await.unwrap();
    let append_data_files = writer.close().await.unwrap();

    // DELETE
    println!(
        "schema: {:?}",
        schema_to_arrow_schema(table.metadata().current_schema())
    );
    let config = EqualityDeleteWriterConfig::new(
        vec![1],
        table.metadata().current_schema().to_owned(),
        None,
    )
    .unwrap();
    let delete_schema = config.projected_arrow_schema_ref();
    let delete_schema = arrow_schema_to_schema(delete_schema).unwrap();

    // Ideally you would use the `delete_schema` with `RecordBatch::try_new` method
    let delete_record_batch = record_batch!(("name", Utf8, ["A"])).unwrap();

    let mut writer = EqualityDeleteFileWriterBuilder::new(
        ParquetWriterBuilder::new(
            Default::default(),
            Arc::new(delete_schema),
            file_io.clone(),
            loc_gen.clone(),
            file_gen.clone(),
        ),
        config,
    )
    .build()
    .await
    .unwrap();

    let _ = writer.write(delete_record_batch).await.unwrap();
    let delete_data_files = writer.close().await.unwrap();

    // COMMIT
    let mut transaction = Transaction::new(&table).fast_append(None, vec![]).unwrap();
    transaction
        .add_data_files(
            vec![append_data_files, delete_data_files]
                .into_iter()
                .flatten(),
        )
        .unwrap();

    transaction
        .apply()
        .await
        .unwrap()
        .commit(&catalog)
        .await
        .unwrap();

    println!("Job done");
}
