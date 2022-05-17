use crate::datasource::BoxedRecordBatchStream;
use arrow::{
    csv::{reader, Reader},
    datatypes::{Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use futures_async_stream::try_stream;
use std::{fs::File, sync::Arc, usize};

use super::Datasource;

pub struct CsvConfig {
    has_header: bool,
    delimiter: u8,
    infer_schema_max_read_records: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            infer_schema_max_read_records: Some(10),
            batch_size: 1024,
            projection: None,
            datetime_format: None,
        }
    }
}

#[derive(Debug)]
pub struct CsvDataSource {
    schema: SchemaRef,
    reader: Reader<File>,
}

impl CsvDataSource {
    pub fn new(filename: String, cfg: &CsvConfig) -> Result<Box<Self>, ArrowError> {
        let schema = Self::infer_schema(filename.clone(), cfg)?;
        let reader = Self::create_reader(filename, schema.clone(), cfg)?;
        Ok(Box::new(CsvDataSource {
            schema: Arc::new(schema),
            reader,
        }))
    }

    fn infer_schema(filename: String, cfg: &CsvConfig) -> Result<Schema, ArrowError> {
        let mut file = File::open(filename)?;
        let (schema, _) = reader::infer_reader_schema(
            &mut file,
            cfg.delimiter,
            cfg.infer_schema_max_read_records,
            cfg.has_header,
        )?;
        Ok(schema)
    }

    fn create_reader(
        filename: String,
        schema: Schema,
        cfg: &CsvConfig,
    ) -> Result<Reader<File>, ArrowError> {
        let file = File::open(filename)?;
        let reader = Reader::new(
            file,
            Arc::new(schema),
            cfg.has_header,
            Some(cfg.delimiter),
            cfg.batch_size,
            None,
            cfg.projection.clone(),
            cfg.datetime_format.clone(),
        );
        Ok(reader)
    }
}

impl CsvDataSource {
    #[try_stream(boxed, ok=RecordBatch, error=ArrowError)]
    async fn do_execute(mut self: Box<Self>) {
        for batch in self.reader.by_ref() {
            yield batch?;
        }
    }
}

impl Datasource for CsvDataSource {
    fn schema(self: Box<Self>) -> SchemaRef {
        self.schema.clone()
    }

    fn execute(self: Box<Self>) -> BoxedRecordBatchStream {
        self.do_execute()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{pin_mut, stream::StreamExt};

    #[tokio::test]
    async fn test_csv_datasource_works() {
        let filename = "./tests/yellow_tripdata_2019-01.csv".to_string();
        let csv_ds = CsvDataSource::new(filename, &CsvConfig::default()).unwrap();
        let stream = csv_ds.execute();
        pin_mut!(stream);
        let batch = stream.next().await;
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert!(batch.is_ok());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 1024);
    }
}
