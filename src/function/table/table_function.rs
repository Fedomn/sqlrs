use std::fmt::Debug;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;

use super::{SeqTableScanBindInput, SeqTableScanInitInput};
use crate::function::{FunctionData, FunctionError};
use crate::main_entry::ClientContext;

pub enum GlobalTableFunctionState {
    None,
}

pub enum TableFunctionBindInput {
    SeqTableScanBindInput(Box<SeqTableScanBindInput>),
    None,
}

#[derive(new)]
pub struct TableFunctionInput {
    pub(crate) bind_data: FunctionData,
    #[allow(dead_code)]
    pub(crate) global_state: GlobalTableFunctionState,
}

pub enum TableFunctionInitInput {
    SeqTableScanInitInput(Box<SeqTableScanInitInput>),
    None,
}

pub type TableFunctionBindFunc =
    fn(TableFunctionBindInput) -> Result<Option<FunctionData>, FunctionError>;

pub type TableFunc =
    fn(Arc<ClientContext>, &mut TableFunctionInput) -> Result<Option<RecordBatch>, FunctionError>;

pub type TableFunctionInitGlobalFunc = fn(
    Arc<ClientContext>,
    TableFunctionInitInput,
) -> Result<GlobalTableFunctionState, FunctionError>;

#[derive(new, Clone)]
pub struct TableFunction {
    // The name of the function
    pub(crate) name: String,
    /// Bind function
    /// This function is used for determining the return type of a table producing function and
    /// returning bind data The returned FunctionData object should be constant and should not
    /// be changed during execution.
    pub(crate) bind: Option<TableFunctionBindFunc>,
    /// (Optional) global init function
    /// Initialize the global operator state of the function.
    /// The global operator state is used to keep track of the progress in the table function and
    /// is shared between all threads working on the table function.
    pub(crate) init_global: Option<TableFunctionInitGlobalFunc>,
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
