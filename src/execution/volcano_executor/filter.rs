use std::sync::Arc;

use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::common::as_boolean_array;
use crate::execution::{
    BoxedExecutor, ExecutionContext, ExecutorError, ExpressionExecutor, PhysicalFilter,
};

#[derive(new)]
pub struct Filter {
    pub(crate) plan: PhysicalFilter,
    pub(crate) child: BoxedExecutor,
}

impl Filter {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let exprs = self.plan.base.expressioins;

        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let eval_mask = ExpressionExecutor::execute(&exprs, &batch)?;
            let predicate = as_boolean_array(&eval_mask[0])?;
            yield filter_record_batch(&batch, predicate)?;
        }
    }
}
