use derive_new::new;

use super::{BoundExpression, BoundExpressionBase};
use crate::function::{CastFunction, DefaultCastFunctions};
use crate::planner_v2::BindError;
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct BoundCastExpression {
    pub(crate) base: BoundExpressionBase,
    /// The child type
    pub(crate) child: Box<BoundExpression>,
    /// Whether to use try_cast or not. try_cast converts cast failures into NULLs instead of
    /// throwing an error.
    pub(crate) try_cast: bool,
    /// The cast function to execute
    pub(crate) function: CastFunction,
}

impl BoundCastExpression {
    pub fn add_cast_to_type(
        expr: BoundExpression,
        target_type: LogicalType,
        alias: String,
        try_cast: bool,
    ) -> Result<BoundExpression, BindError> {
        // TODO: enhance alias to reduce outside alias assignment
        let source_type = expr.return_type();
        assert!(source_type != target_type);
        let cast_function = DefaultCastFunctions::get_cast_function(&source_type, &target_type)?;
        let base = BoundExpressionBase::new(alias, target_type);
        Ok(BoundExpression::BoundCastExpression(
            BoundCastExpression::new(base, Box::new(expr), try_cast, cast_function),
        ))
    }
}
