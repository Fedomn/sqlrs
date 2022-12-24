use super::BoundExpression;

pub struct ExpressionIterator;

impl ExpressionIterator {
    pub fn enumerate_children<F>(expr: &mut BoundExpression, callback: F)
    where
        F: Fn(&mut BoundExpression),
    {
        match expr {
            BoundExpression::BoundColumnRefExpression(_)
            | BoundExpression::BoundConstantExpression(_)
            | BoundExpression::BoundReferenceExpression(_) => {
                // these node types have no children
            }
            BoundExpression::BoundCastExpression(e) => callback(&mut e.child),
            BoundExpression::BoundFunctionExpression(e) => e.children.iter_mut().for_each(callback),
            BoundExpression::BoundComparisonExpression(e) => {
                callback(&mut e.left);
                callback(&mut e.right);
            }
            BoundExpression::BoundConjunctionExpression(e) => {
                e.children.iter_mut().for_each(callback)
            }
        }
    }
}
