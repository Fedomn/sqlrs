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

impl BoundConjunctionExpression {
    /// If expressions count larger than 1, build a and conjunction expression, otherwise return the
    /// first expression
    pub fn try_build_and_conjunction_expression(
        expressions: Vec<BoundExpression>,
    ) -> BoundExpression {
        assert!(!expressions.is_empty());
        // conjuct expression with and make only one expression
        if expressions.len() > 1 {
            let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
            let and_func = DefaultConjunctionFunctions::get_conjunction_function(
                &sqlparser::ast::BinaryOperator::And,
            )
            .unwrap();
            BoundExpression::BoundConjunctionExpression(BoundConjunctionExpression::new(
                base,
                and_func,
                expressions,
            ))
        } else {
            expressions[0].clone()
        }
    }
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
