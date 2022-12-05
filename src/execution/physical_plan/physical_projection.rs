use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundExpression, LogicalProjection};

#[derive(new, Clone)]
pub struct PhysicalProjection {
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) select_list: Vec<BoundExpression>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_projection(&self, op: LogicalProjection) -> PhysicalOperator {
        let new_children = op
            .base
            .children
            .into_iter()
            .map(|p| self.create_plan_internal(p))
            .collect::<Vec<_>>();
        let types = op.base.types;
        let base = PhysicalOperatorBase::new(new_children, types);
        PhysicalOperator::PhysicalProjection(PhysicalProjection::new(base, op.base.expressioins))
    }
}
