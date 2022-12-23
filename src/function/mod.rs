mod errors;
mod scalar;
mod table;

use std::sync::Arc;

use derive_new::new;
pub use errors::*;
pub use scalar::*;
pub use table::*;

use crate::catalog_v2::{Catalog, DEFAULT_SCHEMA};
use crate::common::{CreateInfoBase, CreateScalarFunctionInfo, CreateTableFunctionInfo};
use crate::main_entry::ClientContext;

#[derive(Debug, Clone)]
pub enum FunctionData {
    SeqTableScanInputData(Box<SeqTableScanInputData>),
    SqlrsTablesData(Box<SqlrsTablesData>),
    SqlrsColumnsData(Box<SqlrsColumnsData>),
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

    pub fn add_scalar_functions(
        &mut self,
        function_name: String,
        functions: Vec<ScalarFunction>,
    ) -> Result<(), FunctionError> {
        let info = CreateScalarFunctionInfo::new(
            CreateInfoBase::new(DEFAULT_SCHEMA.to_string()),
            function_name,
            functions,
        );
        Ok(Catalog::create_scalar_function(self.context.clone(), info)?)
    }

    pub fn initialize(&mut self) -> Result<(), FunctionError> {
        SqlrsTablesFunc::register_function(self)?;
        SqlrsColumnsFunc::register_function(self)?;
        AddFunction::register_function(self)?;
        SubtractFunction::register_function(self)?;
        MultiplyFunction::register_function(self)?;
        DivideFunction::register_function(self)?;
        Ok(())
    }
}
