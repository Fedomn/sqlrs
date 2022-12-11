use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::LogicalDummyScan;

#[derive(new, Clone)]
pub struct PhysicalDummyScan {
    pub(crate) base: PhysicalOperatorBase,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_dummy_scan(&self, op: LogicalDummyScan) -> PhysicalOperator {
        let base = PhysicalOperatorBase::new(vec![], op.base.types);
        PhysicalOperator::PhysicalDummyScan(PhysicalDummyScan::new(base))
    }
}
