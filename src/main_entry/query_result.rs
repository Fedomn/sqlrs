use arrow::record_batch::RecordBatch;
use derive_new::new;

use crate::types_v2::LogicalType;

#[derive(new, Debug)]
pub struct BaseQueryResult {
    /// The SQL types of the result
    pub(crate) types: Vec<LogicalType>,
    /// The names of the result
    pub(crate) names: Vec<String>,
}

#[derive(new)]
pub struct MaterializedQueryResult {
    pub(crate) base: BaseQueryResult,
    pub(crate) collection: Vec<RecordBatch>,
}

pub enum QueryResult {
    MaterializedQueryResult(MaterializedQueryResult),
}
