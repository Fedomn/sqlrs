use crate::planner_v2::{
    BoundColumnRefExpression, BoundExpression, BoundExpressionBase, BoundReferenceExpression,
    ColumnBinding, LogicalOperator, LogicalOperatorVisitor,
};

#[derive(Default)]
pub struct ColumnBindingResolver {
    bindings: Vec<ColumnBinding>,
}

impl LogicalOperatorVisitor for ColumnBindingResolver {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        {
            self.visit_operator_children(op);
            self.visit_operator_expressions(op);
            self.bindings = op.get_column_bindings();
        }
    }

    fn visit_replace_column_ref(&self, expr: &BoundColumnRefExpression) -> Option<BoundExpression> {
        assert!(expr.depth == 0);
        // check the current set of column bindings to see which index corresponds to the column
        // reference
        if let Some(idx) = self.bindings.iter().position(|e| expr.binding == *e) {
            let expr = BoundReferenceExpression::new(
                BoundExpressionBase::new(expr.base.alias.clone(), expr.base.return_type.clone()),
                idx,
            );
            return Some(BoundExpression::BoundReferenceExpression(expr));
        }

        // could not bind the column reference, this should never happen and indicates a bug in the
        // code generate an error message
        panic!(
            "Failed to bind column reference {} [{}.{}] (bindings: {:?}), ",
            expr.base.alias, expr.binding.table_idx, expr.binding.column_idx, self.bindings
        );
    }
}
