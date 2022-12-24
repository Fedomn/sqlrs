use derive_new::new;

use super::{BoundCastExpression, BoundExpression, BoundExpressionBase};
use crate::function::{ComparisonFunction, DefaultComparisonFunctions};
use crate::planner_v2::{BindError, ExpressionBinder};
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct BoundComparisonExpression {
    pub(crate) base: BoundExpressionBase,
    pub(crate) left: Box<BoundExpression>,
    pub(crate) right: Box<BoundExpression>,
    /// The comparison function to execute
    pub(crate) function: ComparisonFunction,
}

impl ExpressionBinder<'_> {
    pub fn bind_comparison_expression(
        &mut self,
        left: &sqlparser::ast::Expr,
        op: &sqlparser::ast::BinaryOperator,
        right: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let mut bound_left = self.bind_expression(left, &mut vec![], &mut vec![])?;
        let mut bound_right = self.bind_expression(right, &mut vec![], &mut vec![])?;
        let left_type = bound_left.return_type();
        let right_type = bound_right.return_type();

        // cast the input types to the same type, now obtain the result type of the input types
        let input_type = LogicalType::max_logical_type(&left_type, &right_type)?;
        bound_left =
            BoundCastExpression::try_add_cast_to_type(bound_left, input_type.clone(), true)?;
        bound_right =
            BoundCastExpression::try_add_cast_to_type(bound_right, input_type.clone(), true)?;

        result_names.push(format!(
            "{}({},{})",
            op,
            bound_left.alias(),
            bound_right.alias()
        ));
        result_types.push(LogicalType::Boolean);
        let function = DefaultComparisonFunctions::get_comparison_function(op, &input_type)?;
        let base = BoundExpressionBase::new("".to_string(), LogicalType::Boolean);
        Ok(BoundExpression::BoundComparisonExpression(
            BoundComparisonExpression::new(
                base,
                Box::new(bound_left),
                Box::new(bound_right),
                function,
            ),
        ))
    }
}
