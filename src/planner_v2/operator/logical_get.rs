use derive_new::new;

use super::LogicalOperatorBase;
use crate::catalog_v2::TableCatalogEntry;
use crate::types_v2::LogicalType;

/// LogicalGet represents a scan operation from a data source
#[derive(new, Debug, Clone)]
pub struct LogicalGet {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
    // TODO: migrate to FunctionData when support TableFunction
    pub(crate) bind_table: TableCatalogEntry,
    /// The types of ALL columns that can be returned by the table function
    pub(crate) returned_types: Vec<LogicalType>,
    /// The names of ALL columns that can be returned by the table function
    pub(crate) names: Vec<String>,
    /// Bound column IDs
    #[new(default)]
    #[allow(dead_code)]
    pub(crate) column_ids: Vec<usize>,
}
