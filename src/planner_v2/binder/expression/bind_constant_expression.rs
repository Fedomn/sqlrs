use derive_new::new;

use super::{BoundExpression, BoundExpressionBase};
use crate::planner_v2::{BindError, ExpressionBinder};
use crate::types_v2::{LogicalType, ScalarValue};

#[derive(new, Debug, Clone)]
pub struct BoundConstantExpression {
    pub(crate) base: BoundExpressionBase,
    pub(crate) value: ScalarValue,
}

impl ExpressionBinder<'_> {
    pub fn bind_constant_expr(
        &self,
        v: &sqlparser::ast::Value,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let scalar: ScalarValue = v.into();
        let base = BoundExpressionBase::new(scalar.to_string(), scalar.get_logical_type());
        result_names.push(base.alias.clone());
        result_types.push(base.return_type.clone());
        let expr =
            BoundExpression::BoundConstantExpression(BoundConstantExpression::new(base, scalar));
        Ok(expr)
    }
}
