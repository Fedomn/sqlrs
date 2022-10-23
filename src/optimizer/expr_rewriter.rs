use crate::binder::BoundExpr;

pub trait ExprRewriter {
    fn rewrite_expr(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Constant(_) => self.rewrite_constant(expr),
            BoundExpr::ColumnRef(_) => self.rewrite_column_ref(expr),
            BoundExpr::InputRef(_) => self.rewrite_input_ref(expr),
            BoundExpr::BinaryOp(_) => self.rewrite_binary_op(expr),
            BoundExpr::TypeCast(_) => self.rewrite_type_cast(expr),
            BoundExpr::AggFunc(_) => self.rewrite_agg_func(expr),
            BoundExpr::Alias(_) => self.rewrite_alias(expr),
            BoundExpr::Subquery(_) => self.rewrite_subquery(expr),
        }
    }

    fn rewrite_constant(&self, _: &mut BoundExpr) {}

    fn rewrite_column_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_input_ref(&self, _: &mut BoundExpr) {}

    fn rewrite_type_cast(&self, _: &mut BoundExpr) {}

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::BinaryOp(e) => {
                self.rewrite_expr(&mut e.left);
                self.rewrite_expr(&mut e.right);
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_agg_func(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::AggFunc(e) => {
                for arg in &mut e.exprs {
                    self.rewrite_expr(arg);
                }
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_alias(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::Alias(e) => {
                self.rewrite_expr(&mut e.expr);
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_subquery(&self, _: &mut BoundExpr) {
        // Do nothing due to BoundSubqueryExpr should be rewritten
    }
}
