use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::function::{FunctionData, TableFunction};
use crate::planner_v2::LogicalGet;
use crate::types_v2::LogicalType;

#[derive(new, Clone)]
pub struct PhysicalTableScan {
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) function: TableFunction,
    pub(crate) bind_data: Option<FunctionData>,
    /// The types of ALL columns that can be returned by the table function
    pub(crate) returned_types: Vec<LogicalType>,
    /// The names of ALL columns that can be returned by the table function
    pub(crate) names: Vec<String>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_table_scan(&self, op: LogicalGet) -> PhysicalOperator {
        let base = self.create_physical_operator_base(op.base);
        let plan =
            PhysicalTableScan::new(base, op.function, op.bind_data, op.returned_types, op.names);
        PhysicalOperator::PhysicalTableScan(plan)
    }
}
