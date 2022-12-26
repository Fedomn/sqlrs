use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::LogicalProjection;

#[derive(new, Clone)]
pub struct PhysicalProjection {
    pub(crate) base: PhysicalOperatorBase,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_projection(&self, op: LogicalProjection) -> PhysicalOperator {
        let base = self.create_physical_operator_base(op.base);
        PhysicalOperator::PhysicalProjection(PhysicalProjection::new(base))
    }
}
