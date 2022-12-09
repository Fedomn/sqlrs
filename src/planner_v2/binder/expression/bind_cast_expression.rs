use derive_new::new;

use super::{BoundExpression, BoundExpressionBase};
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct BoundCastExpression {
    pub(crate) base: BoundExpressionBase,
    /// The child type
    pub(crate) child: Box<BoundExpression>,
    #[allow(dead_code)]
    /// Whether to use try_cast or not. try_cast converts cast failures into NULLs instead of
    /// throwing an error.
    pub(crate) try_cast: bool,
}

impl BoundCastExpression {
    pub fn add_cast_to_type(
        expr: BoundExpression,
        target_type: LogicalType,
        alias: String,
        try_cast: bool,
    ) -> BoundExpression {
        if expr.return_type() == target_type {
            return expr;
        }
        let base = BoundExpressionBase::new(alias, target_type);
        BoundExpression::BoundCastExpression(BoundCastExpression::new(
            base,
            Box::new(expr),
            try_cast,
        ))
    }
}
