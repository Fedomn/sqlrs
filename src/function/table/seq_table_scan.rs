use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;

use super::{TableFunction, TableFunctionBindInput, TableFunctionInput};
use crate::catalog_v2::TableCatalogEntry;
use crate::function::{FunctionData, FunctionError};
use crate::main_entry::ClientContext;
use crate::storage_v2::{LocalStorage, LocalStorageReader};

/// The table scan function represents a sequential scan over one of base tables.
pub struct SeqTableScan;

#[derive(new, Debug, Clone)]
pub struct SeqTableScanInputData {
    pub(crate) bind_table: TableCatalogEntry,
    pub(crate) local_storage_reader: LocalStorageReader,
}

#[derive(new)]
pub struct SeqTableScanBindInput {
    pub(crate) bind_table: TableCatalogEntry,
}

#[derive(new)]
pub struct SeqTableScanInitInput {
    #[allow(dead_code)]
    pub(crate) bind_data: FunctionData,
}

impl SeqTableScan {
    fn seq_table_scan_bind_func(
        input: TableFunctionBindInput,
    ) -> Result<Option<FunctionData>, FunctionError> {
        if let TableFunctionBindInput::SeqTableScanBindInput(bind_input) = input {
            let table = bind_input.bind_table;
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

    fn seq_table_scan_func(
        context: Arc<ClientContext>,
        input: &mut TableFunctionInput,
    ) -> Result<Option<RecordBatch>, FunctionError> {
        if let FunctionData::SeqTableScanInputData(data) = &mut input.bind_data {
            let batch = data.local_storage_reader.next_batch(context);
            Ok(batch)
        } else {
            Err(FunctionError::InternalError(
                "unexpected bind data type".to_string(),
            ))
        }
    }

    pub fn get_function() -> TableFunction {
        TableFunction::new(
            "seq_table_scan".to_string(),
            Some(Self::seq_table_scan_bind_func),
            None,
            Self::seq_table_scan_func,
        )
    }
}
