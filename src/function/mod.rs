mod errors;
mod table;

use std::sync::Arc;

use derive_new::new;
pub use errors::*;
pub use table::*;

use crate::catalog_v2::{Catalog, DEFAULT_SCHEMA};
use crate::common::{CreateInfoBase, CreateTableFunctionInfo};
use crate::main_entry::ClientContext;

#[derive(Debug, Clone)]
pub enum FunctionData {
    SeqTableScanInputData(Box<SeqTableScanInputData>),
    SqlrsTablesData(Box<SqlrsTablesData>),
    Placeholder,
}

#[derive(new)]
pub struct BuiltinFunctions {
    pub(crate) context: Arc<ClientContext>,
}

impl BuiltinFunctions {
    pub fn add_table_functions(&mut self, function: TableFunction) -> Result<(), FunctionError> {
        let info = CreateTableFunctionInfo::new(
            CreateInfoBase::new(DEFAULT_SCHEMA.to_string()),
            function.name.clone(),
            vec![function],
        );
        Ok(Catalog::create_table_function(self.context.clone(), info)?)
    }

    pub fn initialize(&mut self) -> Result<(), FunctionError> {
        SqlrsTablesFunc::register_function(self)
    }
}
