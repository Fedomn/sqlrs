mod bind_column_ref_expression;
mod bind_constant_expression;
mod bind_reference_expression;
mod column_binding;

pub use bind_column_ref_expression::*;
pub use bind_constant_expression::*;
pub use bind_reference_expression::*;
pub use column_binding::*;
use derive_new::new;

use crate::types_v2::LogicalType;

/// The Expression represents a bound Expression with a return type
#[derive(new, Debug, Clone)]
pub struct BoundExpressionBase {
    /// The alias of the expression,
    pub(crate) alias: String,
    pub(crate) return_type: LogicalType,
}

#[derive(Debug, Clone)]
pub enum BoundExpression {
    BoundColumnRefExpression(BoundColumnRefExpression),
    BoundConstantExpression(BoundConstantExpression),
    BoundReferenceExpression(BoundReferenceExpression),
}

impl BoundExpression {
    pub fn return_type(&self) -> LogicalType {
        match self {
            BoundExpression::BoundColumnRefExpression(expr) => expr.base.return_type.clone(),
            BoundExpression::BoundConstantExpression(expr) => expr.base.return_type.clone(),
            BoundExpression::BoundReferenceExpression(expr) => expr.base.return_type.clone(),
        }
    }

    pub fn alias(&self) -> String {
        match self {
            BoundExpression::BoundColumnRefExpression(expr) => expr.base.alias.clone(),
            BoundExpression::BoundConstantExpression(expr) => expr.base.alias.clone(),
            BoundExpression::BoundReferenceExpression(expr) => expr.base.alias.clone(),
        }
    }
}
