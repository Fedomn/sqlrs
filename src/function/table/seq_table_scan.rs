use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::catalog_v2::TableCatalogEntry;
use crate::function::{FunctionData, FunctionError};
use crate::main_entry::ClientContext;
use crate::storage_v2::{LocalStorage, LocalStorageReader};
use crate::types_v2::LogicalType;

/// The table scan function represents a sequential scan over one of base tables.
pub struct SeqTableScan;

#[derive(new, Debug, Clone)]
pub struct SeqTableScanInputData {
    pub(crate) bind_table: TableCatalogEntry,
    pub(crate) local_storage_reader: LocalStorageReader,
}

impl SeqTableScan {
    #[allow(clippy::ptr_arg)]
    fn bind_func(
        _context: Arc<ClientContext>,
        input: TableFunctionBindInput,
        _return_types: &mut Vec<LogicalType>,
        _return_names: &mut Vec<String>,
    ) -> Result<Option<FunctionData>, FunctionError> {
        if let Some(table) = input.bind_table {
            let res = FunctionData::SeqTableScanInputData(Box::new(SeqTableScanInputData::new(
                table.clone(),
                LocalStorage::create_reader(&table.storage),
            )));
            Ok(Some(res))
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    fn scan_func(
        context: Arc<ClientContext>,
        input: &mut TableFunctionInput,
    ) -> Result<Option<RecordBatch>, FunctionError> {
        if let Some(bind_data) = &mut input.bind_data {
            if let FunctionData::SeqTableScanInputData(data) = bind_data {
                Ok(data.local_storage_reader.next_batch(context))
            } else {
                Err(FunctionError::InternalError(
                    "unexpected bind data type".to_string(),
                ))
            }
        } else {
            Ok(None)
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
