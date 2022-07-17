mod csv;
use std::io;
use std::sync::Arc;

use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
pub use csv::*;

use crate::catalog::RootCatalog;

#[derive(Clone)]
pub enum StorageImpl {
    CsvStorage(Arc<CsvStorage>),
}

pub trait Storage: Sync + Send + 'static {
    type TableType: Table;

    // currently only support create table by file
    fn create_table(&self, id: String, filepath: String) -> Result<(), StorageError>;

    fn get_table(&self, id: String) -> Result<Self::TableType, StorageError>;

    fn get_catalog(&self) -> RootCatalog;
}

pub trait Table: Sync + Send + Clone + 'static {
    type TransactionType: Transaction;

    fn read(&self) -> Result<Self::TransactionType, StorageError>;
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
