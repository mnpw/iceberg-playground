use flume::{Receiver, Sender};
use iceberg_playground::common::{init, DataFilesAvro};
use tokio::task;

#[tokio::main]
async fn main() {
    let (tx, rx) = flume::bounded::<DataFilesAvro>(1_000);

    println!("Initialising table");
    let (catalog, table) = init().await;

    println!("Starting committer");
    let committer_flush_interval = 5;
    let committer_hd = task::spawn(committer::run(
        rx,
        catalog,
        table.clone(),
        committer_flush_interval,
    ));

    println!("Starting writers");
    // Too many writers will lead to CPU starvation for
    // committer. Tokio runtime needs to be tweaked to
    // set high priority for committer task.
    let writer_count = 200;
    let file_per_writer = 100;
    let record_batches_per_file = 300;
    let writer_flush_interval = 2;
    let writer_hd = task::spawn(writer::run(
        tx,
        table.clone(),
        writer_count,
        file_per_writer,
        record_batches_per_file,
        writer_flush_interval,
    ));

    println!("Awaiting");
    writer_hd.await.expect("Failed to join writer handle");
    committer_hd.await.expect("Failed to join writer handle");

    println!("Job finish");
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
