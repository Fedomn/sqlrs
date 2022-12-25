use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundConjunctionExpression, BoundExpression, LogicalFilter};

#[derive(Clone)]
pub struct PhysicalFilter {
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) expression: BoundExpression,
}

impl PhysicalFilter {
    pub fn new(base: PhysicalOperatorBase, expressions: Vec<BoundExpression>) -> Self {
        let expression =
            BoundConjunctionExpression::try_build_and_conjunction_expression(expressions);
        Self { base, expression }
    }
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_filter(&self, op: LogicalFilter) -> PhysicalOperator {
        assert!(op.base.children.len() == 1);
        // TODO: refactor this part to common method
        let new_children = op
            .base
            .children
            .into_iter()
            .map(|p| self.create_plan_internal(p))
            .collect::<Vec<_>>();
        let types = op.base.types;
        let base = PhysicalOperatorBase::new(new_children, types);
        PhysicalOperator::PhysicalFilter(PhysicalFilter::new(base, op.base.expressioins))
    }
}
