use derive_new::new;

use crate::catalog_v2::ColumnDefinition;

#[derive(new, Debug, Clone)]
pub struct CreateTableInfo {
    pub(crate) base: CreateInfoBase,
    /// Table name to insert to
    pub(crate) table: String,
    /// List of columns of the table
    pub(crate) columns: Vec<ColumnDefinition>,
}

#[derive(new, Debug, Clone)]
pub struct CreateInfoBase {
    pub(crate) schema: String,
}
