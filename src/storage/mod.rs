mod csv;
mod memory;
use std::io;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
pub use csv::*;
pub use memory::*;

use crate::catalog::RootCatalog;

#[derive(Clone)]
pub enum StorageImpl {
    CsvStorage(Arc<CsvStorage>),
    #[allow(dead_code)]
    InMemoryStorage(Arc<InMemoryStorage>),
}

pub trait Storage: Sync + Send + 'static {
    type TableType: Table;

    fn create_csv_table(&self, id: String, filepath: String) -> Result<(), StorageError>;

    fn create_mem_table(&self, id: String, data: Vec<RecordBatch>) -> Result<(), StorageError>;

    fn get_table(&self, id: String) -> Result<Self::TableType, StorageError>;

    fn get_catalog(&self) -> RootCatalog;

    fn show_tables(&self) -> Result<RecordBatch, StorageError>;
}

/// Optional bounds of the reader, of the form (offset, limit).
type Bounds = Option<(usize, usize)>;
type Projections = Option<Vec<usize>>;

pub trait Table: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    /// The bounds is applied to the whole data batches, not per batch.
    ///
    /// The projections is column indices.
    fn read(
        &self,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType, StorageError>;
}

// currently we use a transaction to hold csv reader
pub trait Transaction: Sync + Send + 'static {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError>;
}

#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("arrow error")]
    ArrowError(#[from] ArrowError),

    #[error("io error")]
    IoError(#[from] io::Error),

    #[error("table not found: {0}")]
    TableNotFound(String),
}
