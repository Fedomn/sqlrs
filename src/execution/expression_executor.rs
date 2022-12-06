use arrow::array::ArrayRef;
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
        })
    }
}
