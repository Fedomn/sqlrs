// The following idea inspired from datafusion expr part.

use crate::binder::BoundExpr;
use crate::optimizer::ExprVisitor;

// Visitor that find expressions that match a particular predicate
struct ExprFinder<'a, F>
where
    F: Fn(&BoundExpr) -> bool,
{
    test_fn: &'a F,
    exprs: Vec<BoundExpr>,
}

impl<'a, F> ExprFinder<'a, F>
where
    F: Fn(&BoundExpr) -> bool,
{
    fn new(test_fn: &'a F) -> Self {
        Self {
            test_fn,
            exprs: Vec::new(),
        }
    }
}

impl<'a, F> ExprVisitor for ExprFinder<'a, F>
where
    F: Fn(&BoundExpr) -> bool,
{
    fn pre_visit(&mut self, expr: &BoundExpr) {
        if (self.test_fn)(expr) && !self.exprs.contains(expr) {
            self.exprs.push(expr.clone());
        }
    }
}

/// Search an `Expr`, and all of its nested `Expr`'s, for any that pass the
/// provided test. The returned `Expr`'s are deduplicated and returned in order
/// of appearance (depth first).
fn find_exprs_in_expr<F>(expr: &BoundExpr, test_fn: &F) -> Vec<BoundExpr>
where
    F: Fn(&BoundExpr) -> bool,
{
    let mut finder = ExprFinder::new(test_fn);
    finder.visit_expr(expr);
    finder.exprs
}

fn find_exprs_in_exprs<F>(exprs: &[BoundExpr], test_fn: &F) -> Vec<BoundExpr>
where
    F: Fn(&BoundExpr) -> bool,
{
    exprs
        .iter()
        .flat_map(|expr| find_exprs_in_expr(expr, test_fn))
        .fold(vec![], |mut acc, expr| {
            if !acc.contains(&expr) {
                acc.push(expr)
            }
            acc
        })
}

/// Collect all deeply nested `Expr::AggregateFunction` and
/// `Expr::AggregateUDF`. They are returned in order of occurrence (depth
/// first), with duplicates omitted.
pub fn find_aggregate_exprs(exprs: &[BoundExpr]) -> Vec<BoundExpr> {
    find_exprs_in_exprs(exprs, &|nested_expr| {
        matches!(nested_expr, BoundExpr::AggFunc { .. })
    })
}
