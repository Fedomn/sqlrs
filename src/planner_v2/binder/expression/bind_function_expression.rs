use derive_new::new;

use super::{BoundExpression, BoundExpressionBase};
use crate::catalog_v2::{Catalog, DEFAULT_SCHEMA};
use crate::function::ScalarFunction;
use crate::planner_v2::{BindError, ExpressionBinder, FunctionBinder};
use crate::types_v2::LogicalType;

#[derive(new, Debug, Clone)]
pub struct BoundFunctionExpression {
    pub(crate) base: BoundExpressionBase,
    /// The bound function expression
    pub(crate) function: ScalarFunction,
    /// List of child-expressions of the function
    pub(crate) children: Vec<BoundExpression>,
}

impl ExpressionBinder<'_> {
    pub fn bind_function_expression(
        &mut self,
        left: &sqlparser::ast::Expr,
        op: &sqlparser::ast::BinaryOperator,
        right: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let function_name = match op {
            sqlparser::ast::BinaryOperator::Plus => "add",
            sqlparser::ast::BinaryOperator::Minus => "subtract",
            sqlparser::ast::BinaryOperator::Multiply => "multiply",
            sqlparser::ast::BinaryOperator::Divide => "divide",
            other => {
                return Err(BindError::Internal(format!(
                    "unexpected binary operator {} for function expression",
                    other
                )))
            }
        };
        let function = Catalog::get_scalar_function(
            self.binder.clone_client_context(),
            DEFAULT_SCHEMA.to_string(),
            function_name.to_string(),
        )?;
        let mut return_names = vec![];
        let left = self.bind_expression(left, &mut return_names, &mut vec![])?;
        let right = self.bind_expression(right, &mut return_names, &mut vec![])?;
        let func_binder = FunctionBinder::new();
        let bound_function = func_binder.bind_scalar_function(function, vec![left, right])?;
        result_names.push(format!("{}({})", function_name, return_names.join(", ")));
        result_types.push(bound_function.base.return_type.clone());
        Ok(BoundExpression::BoundFunctionExpression(bound_function))
    }
}
