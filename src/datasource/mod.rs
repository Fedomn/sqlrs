mod csv;
use std::io;

pub use csv::*;

use futures::stream::BoxStream;

use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};

pub type BoxedRecordBatchStream = BoxStream<'static, Result<RecordBatch, DataSourceError>>;

pub trait DataSource {
    fn schema(&self) -> SchemaRef;

    fn execute(self: Box<Self>) -> BoxedRecordBatchStream;
}

#[derive(thiserror::Error, Debug)]
pub enum DataSourceError {
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("io error")]
    IoError(#[from] io::Error),
}
