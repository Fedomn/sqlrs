use arrow::array::ArrayRef;
use arrow::compute::{cast_with_options, CastOptions};
use arrow::record_batch::RecordBatch;

use super::ExecutorError;
use crate::planner_v2::BoundExpression;

/// ExpressionExecutor is responsible for executing a set of expressions and storing the result in a
/// data chunk
pub struct ExpressionExecutor;

impl ExpressionExecutor {
    pub fn execute(
        expressions: &[BoundExpression],
        input: &RecordBatch,
    ) -> Result<Vec<ArrayRef>, ExecutorError> {
        let mut result = vec![];
        for expr in expressions.iter() {
            result.push(Self::execute_internal(expr, input)?);
        }
        Ok(result)
    }

    fn execute_internal(
        expr: &BoundExpression,
        input: &RecordBatch,
    ) -> Result<ArrayRef, ExecutorError> {
        Ok(match expr {
            BoundExpression::BoundColumnRefExpression(_) => todo!(),
            BoundExpression::BoundConstantExpression(e) => e.value.to_array(),
            BoundExpression::BoundReferenceExpression(e) => input.column(e.index).clone(),
            BoundExpression::BoundCastExpression(e) => {
                let child_result = Self::execute_internal(&e.child, input)?;
                let to_type = e.base.return_type.clone().into();
                let options = CastOptions { safe: e.try_cast };
                cast_with_options(&child_result, &to_type, &options)?
            }
            BoundExpression::BoundFunctionExpression(e) => {
                let children_result = e
                    .children
                    .iter()
                    .map(|c| Self::execute_internal(c, input))
                    .collect::<Result<Vec<_>, _>>()?;
                let func = e.function.function;
                func(&children_result)?
            }
        })
    }
}
