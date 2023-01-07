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
    /// If source_expr return_type is same type as target_type, return source_expr directly,
    /// otherwise, add a cast expression to the source_expr.
    pub fn try_add_cast_to_type(
        source_expr: BoundExpression,
        target_type: LogicalType,
        try_cast: bool,
    ) -> Result<BoundExpression, BindError> {
        let source_type = source_expr.return_type();
        if source_type == target_type {
            return Ok(source_expr);
        }
        let cast_function = DefaultCastFunctions::get_cast_function(&source_type, &target_type)?;
        let alias = format!("cast({}) as {}", source_expr.alias(), target_type);
        let base = BoundExpressionBase::new(alias, target_type);
        Ok(BoundExpression::BoundCastExpression(
            BoundCastExpression::new(base, Box::new(source_expr), try_cast, cast_function),
        ))
    }
}
