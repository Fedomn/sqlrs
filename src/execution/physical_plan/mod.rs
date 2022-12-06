mod physical_create_table;
mod physical_expression_scan;
mod physical_insert;
mod physical_projection;
mod physical_table_scan;

use derive_new::new;
pub use physical_create_table::*;
pub use physical_expression_scan::*;
pub use physical_insert::*;
pub use physical_projection::*;
pub use physical_table_scan::*;

use crate::types_v2::LogicalType;

#[derive(new, Default, Clone)]
pub struct PhysicalOperatorBase {
    pub(crate) children: Vec<PhysicalOperator>,
    /// The types returned by this physical operator
    pub(crate) _types: Vec<LogicalType>,
}

#[derive(Clone)]
pub enum PhysicalOperator {
    PhysicalCreateTable(PhysicalCreateTable),
    PhysicalExpressionScan(PhysicalExpressionScan),
    PhysicalInsert(PhysicalInsert),
    PhysicalTableScan(PhysicalTableScan),
    PhysicalProjection(PhysicalProjection),
}
