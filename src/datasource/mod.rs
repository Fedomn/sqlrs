mod csv;
pub use csv::*;

use futures::stream::BoxStream;

use arrow::{datatypes::SchemaRef, error::ArrowError, record_batch::RecordBatch};

pub type BoxedRecordBatchStream = BoxStream<'static, Result<RecordBatch, ArrowError>>;

pub trait Datasource {
    fn schema(self: Box<Self>) -> SchemaRef;

    fn execute(self: Box<Self>) -> BoxedRecordBatchStream;
}
