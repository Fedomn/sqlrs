use derive_new::new;

use crate::catalog_v2::ColumnDefinition;
use crate::function::{ScalarFunction, TableFunction};

#[derive(new, Debug, Clone)]
pub struct CreateInfoBase {
    pub(crate) schema: String,
}

#[derive(new, Debug, Clone)]
pub struct CreateTableInfo {
    pub(crate) base: CreateInfoBase,
    /// Table name to insert to
    pub(crate) table: String,
    /// List of columns of the table
    pub(crate) columns: Vec<ColumnDefinition>,
}

#[derive(new)]
pub struct CreateTableFunctionInfo {
    pub(crate) base: CreateInfoBase,
    /// Function name
    pub(crate) name: String,
    /// Functions with different arguments
    pub(crate) functions: Vec<TableFunction>,
}

#[derive(new)]
pub struct CreateScalarFunctionInfo {
    pub(crate) base: CreateInfoBase,
    /// Function name
    pub(crate) name: String,
    /// Functions with different arguments
    pub(crate) functions: Vec<ScalarFunction>,
}
