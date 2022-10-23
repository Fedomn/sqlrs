use crate::binder::{
    BoundAggFunc, BoundAlias, BoundBinaryOp, BoundColumnRef, BoundExpr, BoundInputRef,
    BoundSubqueryExpr, BoundTypeCast,
};
use crate::types::ScalarValue;

pub trait ExprVisitor {
    fn pre_visit(&mut self, _: &BoundExpr) {}

    fn visit_expr(&mut self, expr: &BoundExpr) {
        self.pre_visit(expr);
        match expr {
            BoundExpr::Constant(expr) => self.visit_constant(expr),
            BoundExpr::ColumnRef(expr) => self.visit_column_ref(expr),
            BoundExpr::InputRef(expr) => self.visit_input_ref(expr),
            BoundExpr::BinaryOp(expr) => self.visit_binary_op(expr),
            BoundExpr::TypeCast(expr) => self.visit_type_cast(expr),
            BoundExpr::AggFunc(expr) => self.visit_agg_func(expr),
            BoundExpr::Alias(expr) => self.visit_alias(expr),
            BoundExpr::Subquery(expr) => self.visit_subquery(expr),
        }
    }

    fn visit_constant(&mut self, _: &ScalarValue) {}

    fn visit_column_ref(&mut self, _: &BoundColumnRef) {}

    fn visit_input_ref(&mut self, _: &BoundInputRef) {}

    fn visit_binary_op(&mut self, expr: &BoundBinaryOp) {
        self.visit_expr(&expr.left);
        self.visit_expr(&expr.right);
    }

    fn visit_type_cast(&mut self, expr: &BoundTypeCast) {
        self.visit_expr(&expr.expr);
    }

    fn visit_agg_func(&mut self, expr: &BoundAggFunc) {
        for arg in &expr.exprs {
            self.visit_expr(arg);
        }
    }

    fn visit_alias(&mut self, expr: &BoundAlias) {
        self.visit_expr(&expr.expr);
    }

    fn visit_subquery(&mut self, _: &BoundSubqueryExpr) {
        // Do nothing due to BoundSubqueryExpr should be rewritten
    }
}
