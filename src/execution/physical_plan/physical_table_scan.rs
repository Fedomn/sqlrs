use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::catalog_v2::TableCatalogEntry;
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::LogicalGet;
use crate::types_v2::LogicalType;

#[derive(new, Clone)]
pub struct PhysicalTableScan {
    pub(crate) _base: PhysicalOperatorBase,
    pub(crate) bind_table: TableCatalogEntry,
    /// The types of ALL columns that can be returned by the table function
    pub(crate) returned_types: Vec<LogicalType>,
    /// The names of ALL columns that can be returned by the table function
    pub(crate) names: Vec<String>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_table_scan(&self, op: LogicalGet) -> PhysicalOperator {
        let base = PhysicalOperatorBase::new(vec![], op.base.types);
        let plan = PhysicalTableScan::new(base, op.bind_table, op.returned_types, op.names);
        PhysicalOperator::PhysicalTableScan(plan)
    }
}
