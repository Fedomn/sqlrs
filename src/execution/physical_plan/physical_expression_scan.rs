use derive_new::new;

use super::{PhysicalOperator, PhysicalOperatorBase};
use crate::execution::PhysicalPlanGenerator;
use crate::planner_v2::{BoundExpression, LogicalExpressionGet};
use crate::types_v2::LogicalType;

/// The PhysicalExpressionScan scans a set of expressions
#[derive(new, Clone)]
pub struct PhysicalExpressionScan {
    pub(crate) base: PhysicalOperatorBase,
    /// The types of the expressions
    pub(crate) expr_types: Vec<LogicalType>,
    /// The set of expressions to scan
    pub(crate) expressions: Vec<Vec<BoundExpression>>,
}

impl PhysicalPlanGenerator {
    pub(crate) fn create_physical_expression_scan(
        &self,
        op: LogicalExpressionGet,
    ) -> PhysicalOperator {
        assert!(op.base.children.len() == 1);
        let base = self.create_physical_operator_base(op.base);
        PhysicalOperator::PhysicalExpressionScan(PhysicalExpressionScan::new(
            base,
            op.expr_types,
            op.expressions,
        ))
    }
}
