use super::{
    BoundCastExpression, BoundColumnRefExpression, BoundComparisonExpression,
    BoundConjunctionExpression, BoundConstantExpression, BoundExpression, BoundFunctionExpression,
    BoundReferenceExpression, ExpressionIterator, LogicalOperator,
};

/// Visitor pattern on logical operators, also includes rewrite expression ability.
pub trait LogicalOperatorVisitor {
    fn visit_operator(&mut self, op: &mut LogicalOperator) {
        self.visit_operator_children(op);
        self.visit_operator_expressions(op);
    }

    fn visit_operator_children(&mut self, op: &mut LogicalOperator) {
        for child in op.children_mut() {
            self.visit_operator(child);
        }
    }

    fn visit_operator_expressions(&mut self, op: &mut LogicalOperator) {
        Self::eumerate_expressions(op, |e| self.visit_expression(e))
    }

    fn eumerate_expressions<F>(op: &mut LogicalOperator, callback: F)
    where
        F: Fn(&mut BoundExpression),
    {
        for expr in op.expressions() {
            callback(expr);
        }
    }

    fn visit_expression(&self, expr: &mut BoundExpression) {
        let result = match expr {
            BoundExpression::BoundColumnRefExpression(e) => self.visit_replace_column_ref(e),
            BoundExpression::BoundConstantExpression(e) => self.visit_replace_constant(e),
            BoundExpression::BoundReferenceExpression(e) => self.visit_replace_reference(e),
            BoundExpression::BoundCastExpression(e) => self.visit_replace_cast(e),
            BoundExpression::BoundFunctionExpression(e) => self.visit_function_expression(e),
            BoundExpression::BoundComparisonExpression(e) => self.visit_comparison_expression(e),
            BoundExpression::BoundConjunctionExpression(e) => self.visit_conjunction_expression(e),
        };
        if let Some(new_expr) = result {
            *expr = new_expr;
        } else {
            self.visit_expression_children(expr);
        }
    }

    fn visit_expression_children(&self, expr: &mut BoundExpression) {
        ExpressionIterator::enumerate_children(expr, |e| self.visit_expression(e))
    }

    fn visit_replace_column_ref(&self, _: &BoundColumnRefExpression) -> Option<BoundExpression> {
        None
    }
    fn visit_replace_constant(&self, _: &BoundConstantExpression) -> Option<BoundExpression> {
        None
    }
    fn visit_replace_reference(&self, _: &BoundReferenceExpression) -> Option<BoundExpression> {
        None
    }
    fn visit_replace_cast(&self, _: &BoundCastExpression) -> Option<BoundExpression> {
        None
    }
    fn visit_function_expression(&self, _: &BoundFunctionExpression) -> Option<BoundExpression> {
        None
    }
    fn visit_comparison_expression(
        &self,
        _: &BoundComparisonExpression,
    ) -> Option<BoundExpression> {
        None
    }
    fn visit_conjunction_expression(
        &self,
        _: &BoundConjunctionExpression,
    ) -> Option<BoundExpression> {
        None
    }
}
