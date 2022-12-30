use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures::stream::BoxStream;
use sqlparser::ast::FunctionArg;

use crate::catalog_v2::TableCatalogEntry;
use crate::function::{FunctionData, FunctionResult};
use crate::main_entry::ClientContext;
use crate::types_v2::LogicalType;

#[derive(new, Default)]
pub struct TableFunctionBindInput {
    pub(crate) bind_table: Option<TableCatalogEntry>,
    #[allow(dead_code)]
    pub(crate) func_args: Option<Vec<FunctionArg>>,
}

#[derive(new, Default)]
pub struct TableFunctionInput {
    pub(crate) bind_data: Option<FunctionData>,
}

pub type TableFunctionBindFunc = fn(
    Arc<ClientContext>,
    TableFunctionBindInput,
    &mut Vec<LogicalType>,
    &mut Vec<String>,
) -> FunctionResult<Option<FunctionData>>;

pub type TableFunc = fn(
    Arc<ClientContext>,
    TableFunctionInput,
) -> FunctionResult<BoxStream<'static, FunctionResult<RecordBatch>>>;

#[derive(new, Clone)]
pub struct TableFunction {
    // The name of the function
    pub(crate) name: String,
    /// Bind function
    /// This function is used for determining the return type of a table producing function and
    /// returning bind data The returned FunctionData object should be constant and should not
    /// be changed during execution.
    pub(crate) bind: Option<TableFunctionBindFunc>,
    /// The main function
    pub(crate) function: TableFunc,
}

impl Debug for TableFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableFunction")
            .field("name", &self.name)
            .finish()
    }
}
