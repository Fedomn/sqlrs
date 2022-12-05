use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::catalog_v2::TableCatalogEntry;
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::LogicalInsert;
use crate::types_v2::LogicalType;

#[derive(new, Clone)]
pub struct PhysicalInsert {
    pub(crate) base: PhysicalOperatorBase,
    /// The insertion map ([table_index -> index in result, or INVALID_INDEX if not specified])
    pub(crate) column_index_list: Vec<usize>,
    /// The expected types for the INSERT statement
    pub(crate) expected_types: Vec<LogicalType>,
    pub(crate) table: TableCatalogEntry,
}

impl PhysicalInsert {
    pub fn clone_with_base(&self, base: PhysicalOperatorBase) -> Self {
        Self {
            base,
            column_index_list: self.column_index_list.clone(),
            expected_types: self.expected_types.clone(),
            table: self.table.clone(),
        }
    }
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_insert(&self, op: LogicalInsert) -> PhysicalOperator {
        let new_children = op
            .base
            .children
            .into_iter()
            .map(|op| self.create_plan_internal(op))
            .collect::<Vec<_>>();
        let base = PhysicalOperatorBase::new(new_children, op.base.types);
        PhysicalOperator::PhysicalInsert(PhysicalInsert::new(
            base,
            op.column_index_list,
            op.expected_types,
            op.table,
        ))
    }
}
