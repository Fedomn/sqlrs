use derive_new::new;

use super::LogicalOperatorBase;
use crate::function::{FunctionData, TableFunction};
use crate::types_v2::LogicalType;

/// LogicalGet represents a scan operation from a data source
#[derive(new, Debug, Clone)]
pub struct LogicalGet {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
    /// The function that is called
    pub(crate) function: TableFunction,
    // The bind data of the function
    pub(crate) bind_data: Option<FunctionData>,
    /// The types of ALL columns that can be returned by the table function
    pub(crate) returned_types: Vec<LogicalType>,
    /// The names of ALL columns that can be returned by the table function
    pub(crate) names: Vec<String>,
    /// Bound column IDs
    #[new(default)]
    #[allow(dead_code)]
    pub(crate) column_ids: Vec<usize>,
}
