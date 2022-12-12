use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundCreateTableInfo, LogicalCreateTable};

#[derive(new, Clone)]
pub struct PhysicalCreateTable {
    #[new(default)]
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) info: BoundCreateTableInfo,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_create_table(&self, op: LogicalCreateTable) -> PhysicalOperator {
        PhysicalOperator::PhysicalCreateTable(PhysicalCreateTable::new(op.info))
    }
}
