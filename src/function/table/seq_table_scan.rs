use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures::stream::BoxStream;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::catalog_v2::TableCatalogEntry;
use crate::function::{FunctionData, FunctionError, FunctionResult};
use crate::main_entry::ClientContext;
use crate::storage_v2::LocalStorage;
use crate::types_v2::LogicalType;

/// The table scan function represents a sequential scan over one of base tables.
pub struct SeqTableScan;

#[derive(new, Debug, Clone)]
pub struct SeqTableScanInputData {
    pub(crate) bind_table: TableCatalogEntry,
}

impl SeqTableScan {
    #[allow(clippy::ptr_arg)]
    fn bind_func(
        _context: Arc<ClientContext>,
        input: TableFunctionBindInput,
        _return_types: &mut Vec<LogicalType>,
        _return_names: &mut Vec<String>,
    ) -> FunctionResult<Option<FunctionData>> {
        if let Some(table) = input.bind_table {
            let res =
                FunctionData::SeqTableScanInputData(Box::new(SeqTableScanInputData::new(table)));
            Ok(Some(res))
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    fn scan_func(
        context: Arc<ClientContext>,
        input: TableFunctionInput,
    ) -> FunctionResult<BoxStream<'static, FunctionResult<RecordBatch>>> {
        if let Some(FunctionData::SeqTableScanInputData(data)) = input.bind_data {
            let mut reader = LocalStorage::create_reader(&data.bind_table.storage);
            let stream = Box::pin(async_stream::try_stream! {
                while let Some(batch) = reader.next_batch(context.clone()){
                    yield batch;
                }
            });
            Ok(stream)
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    pub fn get_function() -> TableFunction {
        TableFunction::new(
            "seq_table_scan".to_string(),
            Some(Self::bind_func),
            Self::scan_func,
        )
    }
}
