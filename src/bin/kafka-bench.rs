use flume::{Receiver, Sender};
use iceberg_playground::common::{init, DataFilesAvro};
use tokio::task;
use arrow::array::{StringArray, Int64Array, Float64Array, BooleanArray, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message;
use serde_json::Value;
use std::collections::HashMap;
use futures::stream::StreamExt; // Ensure that the `futures` crate is added to `Cargo.toml`


//run kafka 
//docker run -d --name=kafka -p 9092:9092 \
// -e KAFKA_LISTENERS=HOST://0.0.0.0:9092,DOCKER://0.0.0.0:9093 \
// -e KAFKA_ADVERTISED_LISTENERS=HOST://localhost:9092,DOCKER://kafka:9093 \
// -e KAFKA_PROCESS_ROLES=broker,controller \
// -e KAFKA_NODE_ID=1 \
// apache/kafka

//create topic with 5 partitions
//docker exec -ti kafka /opt/kafka/bin/kafka-topics.sh \                                              
// --create \
// --bootstrap-server :9092 \
// --partitions 5 \
// --replication-factor 1 \
// --topic demo-1

//send events docker exec -ti kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server :9092 --topic demo-1

#[tokio::main]
async fn main() {
    
    let schema = Arc::new(Schema::new(vec![
        Field::new("name", DataType::Utf8, true),
        Field::new("id", DataType::Int64, true),       
        ]));


    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "my-consumer-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .await
        .expect("Consumer creation error");
    
    print!("Subscribing to topic");
    consumer.subscribe(&["demo"]).expect("Can't subscribe to specified topics");
    print!("listening to topic");
    kafka_consumer::spawn_partition_based_tasks("demo-1").await;
    kafka_consumer::consume(consumer, schema.clone()).await;
    
    // let (tx, rx) = flume::bounded::<DataFilesAvro>(1_000);

    // println!("Initialising table");
    // let (catalog, table) = init().await;

    // println!("Starting committer");
    // let committer_flush_interval = 5;
    // let committer_hd = task::spawn(committer::run(
    //     rx,
    //     catalog,
    //     table.clone(),
    //     committer_flush_interval,
    // ));

    // println!("Starting writers");
    // // Too many writers will lead to CPU starvation for
    // // committer. Tokio runtime needs to be tweaked to
    // // set high priority for committer task.
    // let writer_count = 200;
    // let file_per_writer = 100;
    // let record_batches_per_file = 300;
    // let writer_flush_interval = 2;
    // let writer_hd = task::spawn(writer::run(
    //     tx,
    //     table.clone(),
    //     writer_count,
    //     file_per_writer,
    //     record_batches_per_file,
    //     writer_flush_interval,
    // ));

    // println!("Awaiting");
    // writer_hd.await.expect("Failed to join writer handle");
    // committer_hd.await.expect("Failed to join writer handle");

    println!("Job finish");

}

mod kafka_consumer {
    use super::*;

    use rdkafka::consumer::StreamConsumer;
    use rdkafka::config::ClientConfig;
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use std::time::Duration;
    use rdkafka::consumer::Consumer;
    use futures::stream::StreamExt;


    // Add this function to get partition information and spawn tasks
    pub async fn spawn_partition_based_tasks(
        topic_name: &str
    )  {
        // Create admin client to get metadata
        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
            .create()
            .await
            .expect("Admin client creation failed");
        
        // Get metadata for the topic
        let metadata = admin
            .inner()
            .fetch_metadata(Some(topic_name), Duration::from_secs(10))
            .await
            .expect("Failed to fetch metadata");
        
        // Find our topic and get its partition count
        let mut partition_count = 0;
        for topic in metadata.topics() {
            if topic.name() == topic_name {
                partition_count = topic.partitions().len();
                println!("Topic {} has {} partitions", topic_name, partition_count);
                break;
            }
        }
        
        if partition_count == 0 {
            println!("Topic {} not found or has no partitions", topic_name);
            // return Err(format!("Topic {} not found or has no partitions", topic_name).into());
        }
        println!("partition_count: {}", partition_count);
        
        // Spawn a task for each partition
        // let mut task_handles = Vec::with_capacity(partition_count);
        
        // for partition_id in 0..partition_count {
            // Clone what we need to move into the task
            // let topic_name = topic_name.to_string();
            
            // // Spawn a task dedicated to this partition
            // let handle = task::spawn(async move {});
            
            // task_handles.push(handle);
        // }
            // Ok(task_handles)
        }


    //connect to kafka
    pub async fn connect() -> Result<StreamConsumer, String> {
        let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("group.id", "my-consumer-group")
        .set("auto.offset.reset", "earliest")
        .create()
        .await
        .expect("Consumer creation error");

    consumer.subscribe(&["greetings"]).expect("Can't subscribe to specified topics");
        
        Ok(consumer)
    }

    pub async fn consume(consumer: StreamConsumer, schema: Arc<Schema>) -> Result<(), String> {
        
        // while let Some(message_result) = consumer.stream().next().await {
        //     match message_result {
        //         Ok(message) => {
        //             if let Some(Ok(payload)) = message.payload_view::<str>() {
        //                 println!("Message content: {}", payload);
        //             } else {
        //                 eprintln!("Failed to parse message payload as string");
        //             }    
        //             println!("Received message: {:?}", message);
        //         }
        //         Err(e) => {
        //             eprintln!("Error receiving message: {:?}", e);
        //         }
        //     }
        // }

        let mut stream = consumer.stream().ready_chunks(2);
        while let Some(batch) = stream.next().await {
            process_batch(batch, schema.clone()).await;
        }
    
        Ok(())

        // while let Some(message_result)= stream_msg.next().await{ 

        // }
    }
    
    async fn process_batch<'a>(batch: Vec<Result<rdkafka::message::BorrowedMessage<'a>, rdkafka::error::KafkaError>>,schema: Arc<Schema>) {
        // let mut successes = 0;
        let mut errors = 0;
        println!("len of batch: {}", batch.len());
        let mut json_values = Vec::new();

        for msg in batch {
            match msg {
                Ok(message) => {
                    if let Some(Ok(payload)) = message.payload_view::<str>() {
                        match serde_json::from_str::<Value>(payload) {
                            Ok(value) => json_values.push(value),
                            Err(e) => {
                                eprintln!("Failed to parse JSON: {}", e);
                                errors += 1;
                            }
                        }
                    } else {
                        eprintln!("Failed to parse message payload as string");
                        errors += 1;
                    }
                }
                Err(e) => {
                    eprintln!("Message error: {}", e);
                    errors += 1;
                }    
                // Ok(message) => {
                //     if let Some(Ok(payload)) = message.payload_view::<str>() {
                //         println!("Message content: {}", payload);
                //     } else {
                //         eprintln!("Failed to parse message payload as string");
                //     }    
                // }
                // Err(e) => {
                //     eprintln!("Message error: {}", e);
                //     errors += 1;
                // }
            }
        }

        match json_to_arrow(&json_values, schema) {

            Ok(record_batch) => {
                use arrow::util::pretty::pretty_format_batches;
                match pretty_format_batches(&[record_batch]) {
                    Ok(formatted) => println!("{}", formatted),
                    Err(e) => eprintln!("Formatting error: {}", e),
                }
            }
            Err(e) => eprintln!("Failed to convert JSON to Arrow: {}", e),
        }
        
        println!("Processed batch: successes, {} errors", errors);
    }
    

}

fn json_to_arrow(json_messages: &[Value], schema: Arc<Schema>) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    // Create dynamic arrays based on schema fields
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    let mut column_data: HashMap<String, Vec<Option<Value>>> = HashMap::new();
    
    // Initialize column data vectors
    for field in schema.fields() {
        column_data.insert(field.name().clone(), Vec::with_capacity(json_messages.len()));
    }
    
    // Extract field values from JSON messages
    for json in json_messages {
        for field in schema.fields() {
            let field_name = field.name();
            let values = column_data.get_mut(field_name).unwrap();
            
            if let Some(value) = json.get(field_name) {
                values.push(Some(value.clone()));
            } else if !field.is_nullable() {
                return Err(format!("Missing required field: {}", field_name).into());
            } else {
                values.push(None);
            }
        }
    }
    
    // Convert JSON values to Arrow arrays
    for field in schema.fields() {
        let field_name = field.name();
        let values = column_data.get(field_name).unwrap();
        
        // Create appropriate Arrow array based on field type
        match field.data_type() {
            DataType::Int64 => {
                let array_data: Vec<Option<i64>> = values.iter()
                    .map(|opt_value| {
                        opt_value.as_ref().and_then(|v| v.as_i64())
                    })
                    .collect();
                arrays.push(Arc::new(Int64Array::from(array_data)));
            },
            DataType::Utf8 => {
                let array_data: Vec<Option<String>> = values.iter()
                    .map(|opt_value| {
                        opt_value.as_ref().and_then(|v| v.as_str().map(|s| s.to_string()))
                    })
                    .collect();
                arrays.push(Arc::new(StringArray::from(array_data)));
            },
            DataType::Float64 => {
                let array_data: Vec<Option<f64>> = values.iter()
                    .map(|opt_value| {
                        opt_value.as_ref().and_then(|v| v.as_f64())
                    })
                    .collect();
                arrays.push(Arc::new(Float64Array::from(array_data)));
            },
            DataType::Boolean => {
                let array_data: Vec<Option<bool>> = values.iter()
                    .map(|opt_value| {
                        opt_value.as_ref().and_then(|v| v.as_bool())
                    })
                    .collect();
                arrays.push(Arc::new(BooleanArray::from(array_data)));
            },
            dt => return Err(format!("Unsupported data type in conversion: {:?}", dt).into()),
        }
    }
    
    // Create a RecordBatch
    let batch = RecordBatch::try_new(schema, arrays)?;
    
    Ok(batch)
}


mod writer {
    use super::*;

    use std::time::{Duration, Instant};

    use arrow::array::{record_batch, RecordBatch};
    use iceberg::{
        spec::{write_data_files_to_avro, DataFile, DataFileFormat, TableMetadataRef},
        table::Table,
        writer::{
            base_writer::data_file_writer::DataFileWriterBuilder,
            file_writer::{
                location_generator::{DefaultFileNameGenerator, DefaultLocationGenerator},
                ParquetWriterBuilder,
            },
            IcebergWriter, IcebergWriterBuilder,
        },
    };
    use iceberg_playground::common::next_instant;
    use rand::Rng;

    pub fn gen_record_batch(length: usize) -> Vec<RecordBatch> {
        // TODO: randomize
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

        vec![rb; length]
    }

    pub fn serialize(
        input: &mut Vec<DataFile>,
        output: &mut DataFilesAvro,
        table_metadata_ref: &TableMetadataRef,
    ) {
        let data = std::mem::take(input);

        let _ = write_data_files_to_avro(
            output,
            data,
            table_metadata_ref.default_partition_type(),
            table_metadata_ref.format_version(),
        )
        .expect("Failed to write data files into avro");
    }

    pub fn flush(
        tx: &Sender<DataFilesAvro>,
        data_files: &mut Vec<DataFile>,
        table_metadata_ref: &TableMetadataRef,
    ) {
        if data_files.is_empty() {
            return;
        }

        let mut payload = Vec::new();
        serialize(data_files, &mut payload, table_metadata_ref);
        tx.send(payload).expect("Failed to send to committer");
    }

    pub async fn writer(
        writer_builder: impl IcebergWriterBuilder,
        table_metadata_ref: TableMetadataRef,
        tx: Sender<DataFilesAvro>,
        file_count: usize,
        batches_per_file: usize,
        flush_duration: usize,
    ) {
        let mut tick = next_instant(flush_duration);
        let mut data_files = Vec::new();
        let mut file_iter = 0..file_count;

        loop {
            // Flush
            if tick > Instant::now() {
                flush(&tx, &mut data_files, &table_metadata_ref);
                tick = next_instant(flush_duration);
            }

            // Exit if done
            if let None = file_iter.next() {
                flush(&tx, &mut data_files, &table_metadata_ref);
                break;
            }

            // Write parquet data file
            {
                let mut writer = writer_builder
                    .clone()
                    .build()
                    .await
                    .expect("Failed to build writer");

                for rb in gen_record_batch(batches_per_file) {
                    writer.write(rb).await.expect("Failed to write");
                }

                // simulate variable network
                let sleep_millis = rand::rng().random_range(100..200);
                tokio::time::sleep(Duration::from_millis(sleep_millis)).await;

                data_files.extend(writer.close().await.expect("Failed to close writer"));
            }
        }
    }

    pub async fn run(
        tx: Sender<DataFilesAvro>,
        table: Table,
        writer_count: usize,
        files_per_writer: usize,
        rb_per_file: usize,
        flush_duration: usize,
    ) {
        let mut writers_hd = Vec::new();
        for i in 0..writer_count {
            // Create data file writer builder
            let data_file_writer_builder = {
                let schema = table.metadata().current_schema().to_owned();
                let file_io = table.file_io().clone();
                let loc_gen = DefaultLocationGenerator::new(table.metadata().to_owned())
                    .expect("Failed to create location generator");
                let file_name_gen =
                    DefaultFileNameGenerator::new(format!("{i}"), None, DataFileFormat::Parquet);

                let data_file_writer_builder = DataFileWriterBuilder::new(
                    ParquetWriterBuilder::new(
                        Default::default(),
                        schema,
                        file_io,
                        loc_gen,
                        file_name_gen,
                    ),
                    None,
                );

                data_file_writer_builder
            };

            // Spawn a writer
            let hd = task::spawn(writer(
                data_file_writer_builder,
                table.metadata_ref(),
                tx.clone(),
                files_per_writer,
                rb_per_file,
                flush_duration,
            ));
            writers_hd.push(hd);
        }

        for hd in writers_hd {
            let _ = hd.await;
        }

        println!("Sent data files!");
    }
}

mod committer {
    use std::{io::Cursor, mem, time::Instant};

    use iceberg::{
        spec::{read_data_files_from_avro, DataFile},
        table::Table,
        transaction::Transaction,
        Catalog,
    };
    use iceberg_playground::common::next_instant;

    use super::*;

    pub async fn run(
        rx: Receiver<DataFilesAvro>,
        catalog: impl Catalog,
        mut table: Table,
        flush_duration: usize,
    ) {
        let mut tick = next_instant(flush_duration);
        let mut batch = Vec::<DataFile>::new();

        loop {
            // Flush
            if Instant::now() > tick {
                flush(&mut batch, &catalog, &mut table).await;
                tick = next_instant(flush_duration);
                println!("next flush – {tick:?}");
            }

            // Exit if done
            let data = match rx.try_recv() {
                Ok(data) => data,
                Err(flume::TryRecvError::Disconnected) => {
                    flush(&mut batch, &catalog, &mut table).await;
                    break;
                }
                Err(_) => continue,
            };

            // Batch data files
            {
                let mut reader = Cursor::new(data);
                let data_files = read_data_files_from_avro(
                    &mut reader,
                    table.metadata().current_schema(),
                    table.metadata().default_partition_type(),
                    table.metadata().format_version(),
                )
                .expect("Failed to read data files from avro");

                batch.extend(data_files);
            }
        }

        println!("All data files committed!")
    }

    async fn flush(batch: &mut Vec<DataFile>, catalog: &impl Catalog, table: &mut Table) {
        if batch.is_empty() {
            return;
        }

        println!(
            "[{:?}] Commit start: {} data files",
            Instant::now(),
            batch.len(),
        );

        let data_files = mem::take(batch);
        println!("[{:?}] Transaction start", Instant::now(),);
        let mut action = Transaction::new(&table)
            .fast_append(None, vec![])
            .expect("Failed to create transaction");
        action
            .add_data_files(data_files)
            .expect("Failed to add data files");
        println!("[{:?}] Transaction done", Instant::now(),);
        println!("[{:?}] Commit start", Instant::now(),);
        *table = action
            .apply()
            .await
            .expect("Failed to apply action")
            .commit(catalog)
            .await
            .expect("Failed to commit insert");

        println!("[{:?}] Commit done", Instant::now());
    }
}