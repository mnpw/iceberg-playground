use std::{collections::HashMap, sync::Arc};

use arrow::array::{record_batch, RecordBatch};
use iceberg::{
    io::{S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
    spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type},
    transaction::Transaction,
    writer::{
        base_writer::data_file_writer::DataFileWriterBuilder,
        file_writer::{
            location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
            ParquetWriterBuilder,
        },
        IcebergWriter, IcebergWriterBuilder,
    },
    Catalog, NamespaceIdent, TableCreation, TableIdent,
};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};

#[tokio::main]
async fn main() {
    let config = RestCatalogConfig::builder()
        .uri("http://localhost:8181".to_owned())
        .props(HashMap::from([
            (S3_ENDPOINT.to_owned(), "http://localhost:9000".to_owned()),
            (S3_REGION.to_owned(), "us-east-1".to_owned()),
            (S3_ACCESS_KEY_ID.to_owned(), "admin".to_owned()),
            (S3_SECRET_ACCESS_KEY.to_owned(), "password".to_owned()),
            // because minIO is local bucket, we don't need to sign the requests
            (S3_ALLOW_ANONYMOUS.to_owned(), "true".to_owned()),
            // (AWS_ACCESS_KEY_ID.to_owned(), config.access_key_id),
            // (AWS_SECRET_ACCESS_KEY.to_owned(), config.secret_access_key),
            // (AWS_SESSION_TOKEN.to_owned(), config.session_token),
            // (AWS_REGION_NAME.to_string(), config.region),
        ]))
        .build();

    let catalog = RestCatalog::new(config);
    let ns = NamespaceIdent::new("test-ns".to_owned());
    let tb = TableIdent::new(ns, "test-table-1".to_owned());
    init(&catalog, &tb).await;

    // insert into table
    let t = catalog.load_table(&tb).await.expect("Failed to load table");
    let schema = t.metadata().current_schema().to_owned();
    let file_io = t.file_io().clone();
    let loc_gen = DefaultLocationGenerator::new(t.metadata().to_owned())
        .expect("Failed to create location generator");
    let file_name_gen =
        DefaultFileNameGenerator::new("TEST".to_owned(), None, DataFileFormat::Parquet);
    // NOTE: this would become invalid when schema or data write directory changes
    let file_writer_builder =
        ParquetWriterBuilder::new(Default::default(), schema, file_io, loc_gen, file_name_gen);
    let mut data_file_writer = DataFileWriterBuilder::new(file_writer_builder, None)
        .build()
        .await
        .expect("Failed to build data file writer");
    let rb: RecordBatch = record_batch!(
        ("name", Utf8, ["A", "B", "C", "D", "E"]),
        (
            "type",
            Utf8,
            ["small", "medium", "medium", "small", "large"]
        ),
        ("count", Int32, [2, 15, 10, 20, 20])
    )
    .expect("Failed to create record batch");

    data_file_writer
        .write(rb)
        .await
        .expect("Failed to write to iceberg");

    let data_files = data_file_writer
        .close()
        .await
        .expect("Failed to close the writer");

    let mut action = Transaction::new(&t)
        .fast_append(None, vec![])
        .expect("Failed to create transaction");

    action
        .add_data_files(data_files)
        .expect("Failed to add data files");

    let table = action
        .apply()
        .await
        .expect("Failed to apply action")
        .commit(&catalog)
        .await
        .expect("Failed to commit insert");

    // println!("namespaces: {ns:?}");
}

async fn init(catalog: &impl Catalog, tb: &TableIdent) {
    // doess this purge table as well?

    let table_exists = catalog
        .table_exists(&tb)
        .await
        .expect("Failed to check existence of table");

    if table_exists {
        println!("Table exists, dropping table");
        catalog.drop_table(&tb).await.expect("Failed to drop table");
    } else {
        println!("Table does not exist");
    }
    println!("Creating table");

    // create table
    let fields = vec![
        NestedField::new(0, "name", Type::Primitive(PrimitiveType::String), false),
        NestedField::new(1, "size", Type::Primitive(PrimitiveType::String), false),
        NestedField::new(2, "count", Type::Primitive(PrimitiveType::Int), false),
    ];

    let schema = Schema::builder()
        .with_fields(fields.into_iter().map(Arc::from))
        .build()
        .expect("Failed to build schema");

    let table_schema = TableCreation::builder()
        .name(tb.name().to_owned())
        .schema(schema)
        .build();

    let z = catalog
        .create_table(&tb.namespace(), table_schema)
        .await
        .expect("Failed to create table");
}
