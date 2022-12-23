use derive_new::new;

use super::{BindError, BoundExpressionBase, INVALID_INDEX};
use crate::catalog_v2::ScalarFunctionCatalogEntry;
use crate::function::ScalarFunction;
use crate::planner_v2::{BoundExpression, BoundFunctionExpression};
use crate::types_v2::LogicalType;

/// Find the function with matching parameters from the function list.
#[derive(new)]
pub struct FunctionBinder;

impl FunctionBinder {
    pub fn bind_scalar_function(
        &self,
        func: ScalarFunctionCatalogEntry,
        children: Vec<BoundExpression>,
    ) -> Result<BoundFunctionExpression, BindError> {
        let arguments = self.get_logical_types_from_expressions(&children);
        let best_func_idx = self.bind_function_from_arguments(&func, &arguments)?;
        let bound_function = func.functions[best_func_idx].clone();
        let base = BoundExpressionBase::new("".to_string(), bound_function.return_type.clone());
        Ok(BoundFunctionExpression::new(base, bound_function, children))
    }

    fn get_logical_types_from_expressions(&self, children: &[BoundExpression]) -> Vec<LogicalType> {
        children.iter().map(|c| c.return_type()).collect()
    }

    fn bind_function_from_arguments(
        &self,
        func: &ScalarFunctionCatalogEntry,
        arguments: &[LogicalType],
    ) -> Result<usize, BindError> {
        let mut candidate_functions = vec![];
        let mut best_function_idx = INVALID_INDEX;
        for (func_idx, each_func) in func.functions.iter().enumerate() {
            let cost = self.bind_function_cost(each_func, arguments);
            if cost < 0 {
                continue;
            }
            candidate_functions.push(func_idx);
            best_function_idx = func_idx;
        }

        if best_function_idx == INVALID_INDEX {
            return Err(BindError::FunctionBindError(format!(
                "No function matched for given function and arguments {} {:?}",
                func.base.name, arguments
            )));
        }

        if candidate_functions.len() > 1 {
            return Err(BindError::FunctionBindError(format!(
                "Ambiguous function call for function {} and arguments {:?}",
                func.base.name, arguments
            )));
        }

        Ok(candidate_functions[0])
    }

    fn bind_function_cost(&self, func: &ScalarFunction, arguments: &[LogicalType]) -> i32 {
        if func.arguments.len() != arguments.len() {
            // invalid argument count: check the next function
            return -1;
        }
        let cost = 0;
        // TODO: use cast function to infer the cost and choose the best matched function.
        for (i, arg) in arguments.iter().enumerate() {
            if func.arguments[i] != *arg {
                // invalid argument count: check the next function
                return -1;
            }
        }
        cost
    }
}
