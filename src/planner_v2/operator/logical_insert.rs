use derive_new::new;

use super::LogicalOperatorBase;
use crate::catalog_v2::TableCatalogEntry;
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct LogicalInsert {
    pub(crate) base: LogicalOperatorBase,
    /// The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
    pub(crate) column_index_list: Vec<usize>,
    /// The expected types for the INSERT statement
    pub(crate) expected_types: Vec<LogicalType>,
    pub(crate) table: TableCatalogEntry,
}
