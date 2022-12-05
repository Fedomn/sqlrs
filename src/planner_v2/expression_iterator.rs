use super::BoundExpression;

pub struct ExpressionIterator;

impl ExpressionIterator {
    pub fn enumerate_children<F>(expr: &mut BoundExpression, _callback: F)
    where
        F: Fn(&mut BoundExpression),
    {
        match expr {
            BoundExpression::BoundColumnRefExpression(_)
            | BoundExpression::BoundConstantExpression(_)
            | BoundExpression::BoundReferenceExpression(_) => {
                // these node types have no children
            }
        }
    }
}
