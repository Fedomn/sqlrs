use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::LogicalLimit;

#[derive(new, Clone)]
pub struct PhysicalLimit {
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) limit: Option<u64>,
    pub(crate) offset: Option<u64>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_limit(&self, op: LogicalLimit) -> PhysicalOperator {
        let base = self.create_physical_operator_base(op.base);
        let limit = op.limit.map(|_| op.limit_value);
        let offset = op.offset.map(|_| op.offsert_value);
        PhysicalOperator::PhysicalLimit(PhysicalLimit::new(base, limit, offset))
    }
}
