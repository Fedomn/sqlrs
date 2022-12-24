use derive_new::new;

use super::{BindError, BoundCastExpression, BoundExpressionBase, INVALID_INDEX};
use crate::catalog_v2::ScalarFunctionCatalogEntry;
use crate::function::{CastRules, ScalarFunction};
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
        // bind the function
        let arguments = self.get_logical_types_from_expressions(&children);
        // found a matching function!
        let best_func_idx = self.bind_function_from_arguments(&func, &arguments)?;
        let bound_function = func.functions[best_func_idx].clone();
        // check if we need to add casts to the children
        let new_children = self.cast_to_function_arguments(&bound_function, children)?;
        // now create the function
        let base = BoundExpressionBase::new("".to_string(), bound_function.return_type.clone());
        Ok(BoundFunctionExpression::new(
            base,
            bound_function,
            new_children,
        ))
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
        let mut lowest_cost = i32::MAX;
        for (func_idx, each_func) in func.functions.iter().enumerate() {
            // check the arguments of the function
            let cost = self.bind_function_cost(each_func, arguments);
            if cost < 0 {
                // auto casting was not possible
                continue;
            }
            if cost == lowest_cost {
                // we have multiple functions with the same cost, so just add it to the candidates
                candidate_functions.push(func_idx);
                continue;
            }
            if cost > lowest_cost {
                // we have a function with a higher cost, so skip it
                continue;
            }
            // we have a function with a lower cost, so clear the candidates and add this one
            candidate_functions.clear();
            lowest_cost = cost;
            best_function_idx = func_idx;
            candidate_functions.push(best_function_idx);
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
        let mut cost = 0;
        for (i, arg) in arguments.iter().enumerate() {
            if func.arguments[i] != *arg {
                // invalid argument count: check the next function
                let cast_cost = CastRules::implicit_cast_cost(arg, &func.arguments[i]);
                if cast_cost >= 0 {
                    // we can implicitly cast, add the cost to the total cost
                    cost += cast_cost;
                } else {
                    // we can't implicitly cast
                    return -1;
                }
            }
        }
        cost
    }

    fn cast_to_function_arguments(
        &self,
        bound_function: &ScalarFunction,
        children: Vec<BoundExpression>,
    ) -> Result<Vec<BoundExpression>, BindError> {
        let mut new_children = vec![];
        for (i, child) in children.into_iter().enumerate() {
            let target_type = &bound_function.arguments[i];
            new_children.push(BoundCastExpression::try_add_cast_to_type(
                child,
                target_type.clone(),
                true,
            )?)
        }
        Ok(new_children)
    }
}
