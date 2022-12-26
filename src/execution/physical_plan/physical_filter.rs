use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundConjunctionExpression, LogicalFilter};

#[derive(Clone)]
pub struct PhysicalFilter {
    pub(crate) base: PhysicalOperatorBase,
}

impl PhysicalFilter {
    pub fn new(mut base: PhysicalOperatorBase) -> Self {
        let expression =
            BoundConjunctionExpression::try_build_and_conjunction_expression(base.expressioins);
        base.expressioins = vec![expression];
        Self { base }
    }
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_filter(&self, op: LogicalFilter) -> PhysicalOperator {
        assert!(op.base.children.len() == 1);
        let base = self.create_physical_operator_base(op.base);
        PhysicalOperator::PhysicalFilter(PhysicalFilter::new(base))
    }
}
