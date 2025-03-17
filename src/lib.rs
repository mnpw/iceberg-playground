pub mod common {
    use iceberg::{
        io::{S3_ACCESS_KEY_ID, S3_ALLOW_ANONYMOUS, S3_ENDPOINT, S3_REGION, S3_SECRET_ACCESS_KEY},
        spec::{NestedField, PrimitiveType, Schema, Type},
        table::Table,
        Catalog, TableCreation,
    };
    use iceberg_catalog_rest::RestCatalogConfig;
    use std::{
        collections::HashMap,
        sync::Arc,
        time::{Duration, Instant},
    };

    use iceberg::{NamespaceIdent, TableIdent};

    use iceberg_catalog_rest::RestCatalog;

    pub type DataFilesAvro = Vec<u8>;

    pub async fn init() -> (RestCatalog, Table) {
        let config = RestCatalogConfig::builder()
            .uri("http://localhost:8181".to_owned())
            .props(HashMap::from([
                (S3_ENDPOINT.to_owned(), "http://localhost:9000".to_owned()),
                (S3_REGION.to_owned(), "us-east-1".to_owned()),
                (S3_ACCESS_KEY_ID.to_owned(), "admin".to_owned()),
                (S3_SECRET_ACCESS_KEY.to_owned(), "password".to_owned()),
                (S3_ALLOW_ANONYMOUS.to_owned(), "true".to_owned()),
            ]))
            .build();

        let catalog = RestCatalog::new(config);
        let ns = NamespaceIdent::new("test-ns".to_owned());
        let tb = TableIdent::new(ns, "test-table-1".to_owned());
        let table = create_table(&catalog, &tb).await;

        return (catalog, table);
    }

    async fn create_table(catalog: &impl Catalog, tb: &TableIdent) -> Table {
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

        catalog
            .create_table(&tb.namespace(), table_schema)
            .await
            .expect("Failed to create table")
    }

    pub fn next_instant(duration: usize) -> Instant {
        Instant::now() + Duration::from_secs(duration as u64)
    }
}
