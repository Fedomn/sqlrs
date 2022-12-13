mod physical_column_data_scan;
mod physical_create_table;
mod physical_dummy_scan;
mod physical_explain;
mod physical_expression_scan;
mod physical_insert;
mod physical_projection;
mod physical_table_scan;

use derive_new::new;
pub use physical_column_data_scan::*;
pub use physical_create_table::*;
pub use physical_dummy_scan::*;
pub use physical_explain::*;
pub use physical_expression_scan::*;
pub use physical_insert::*;
pub use physical_projection::*;
pub use physical_table_scan::*;

use crate::types_v2::LogicalType;

#[derive(new, Default, Clone)]
pub struct PhysicalOperatorBase {
    pub(crate) children: Vec<PhysicalOperator>,
    /// The types returned by this physical operator
    pub(crate) types: Vec<LogicalType>,
}

#[derive(Clone)]
pub enum PhysicalOperator {
    PhysicalCreateTable(PhysicalCreateTable),
    PhysicalDummyScan(PhysicalDummyScan),
    PhysicalExpressionScan(PhysicalExpressionScan),
    PhysicalInsert(PhysicalInsert),
    PhysicalTableScan(PhysicalTableScan),
    PhysicalProjection(PhysicalProjection),
    PhysicalColumnDataScan(PhysicalColumnDataScan),
}

impl PhysicalOperator {
    pub fn children(&self) -> &[PhysicalOperator] {
        match self {
            PhysicalOperator::PhysicalCreateTable(op) => &op.base.children,
            PhysicalOperator::PhysicalExpressionScan(op) => &op.base.children,
            PhysicalOperator::PhysicalInsert(op) => &op.base.children,
            PhysicalOperator::PhysicalTableScan(op) => &op.base.children,
            PhysicalOperator::PhysicalProjection(op) => &op.base.children,
            PhysicalOperator::PhysicalDummyScan(op) => &op.base.children,
            PhysicalOperator::PhysicalColumnDataScan(op) => &op.base.children,
        }
    }
}
