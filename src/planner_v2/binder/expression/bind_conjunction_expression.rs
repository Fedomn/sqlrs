use derive_new::new;

use super::{BoundCastExpression, BoundExpression, BoundExpressionBase};
use crate::function::{ConjunctionFunction, DefaultConjunctionFunctions};
use crate::planner_v2::{BindError, ExpressionBinder};
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct BoundConjunctionExpression {
    pub(crate) base: BoundExpressionBase,
    pub(crate) function: ConjunctionFunction,
    pub(crate) children: Vec<BoundExpression>,
}

impl ExpressionBinder<'_> {
    pub fn bind_conjunction_expression(
        &mut self,
        left: &sqlparser::ast::Expr,
        op: &sqlparser::ast::BinaryOperator,
        right: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let function = DefaultConjunctionFunctions::get_conjunction_function(op)?;

        let mut return_names = vec![];
        let mut left = self.bind_expression(left, &mut return_names, &mut vec![])?;
        left = BoundCastExpression::try_add_cast_to_type(left, LogicalType::Boolean, true)?;
        return_names[0] = left.alias();
        let mut right = self.bind_expression(right, &mut return_names, &mut vec![])?;
        right = BoundCastExpression::try_add_cast_to_type(right, LogicalType::Boolean, true)?;
        return_names[1] = right.alias();

        result_names.push(format!("{}({},{})", op, return_names[0], return_names[1]));
        result_types.push(LogicalType::Boolean);
        let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
        Ok(BoundExpression::BoundConjunctionExpression(
            BoundConjunctionExpression::new(base, function, vec![left, right]),
        ))
    }
}
