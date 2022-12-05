use derive_new::new;
use sqlparser::ast::Statement;

use crate::execution::PhysicalOperator;
use crate::types_v2::LogicalType;

#[derive(new)]
#[allow(dead_code)]
pub struct PreparedStatementData {
    /// The unbound SQL statement that was prepared
    pub(crate) unbound_statement: Statement,
    /// The fully prepared physical plan of the prepared statement
    pub(crate) plan: PhysicalOperator,
    /// The result names
    pub(crate) names: Vec<String>,
    /// The result types
    pub(crate) types: Vec<LogicalType>,
}
