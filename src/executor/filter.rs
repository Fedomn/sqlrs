use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;

use super::*;
use crate::binder::BoundExpr;

pub struct FilterExecutor {
    pub expr: BoundExpr,
    pub child: BoxedExecutor,
}

impl FilterExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let eval_mask = self.expr.eval_column(&batch)?;
            let predicate = eval_mask
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("filter executor expected evaluate boolean array");
            let batch = filter_record_batch(&batch, predicate)?;
            yield batch;
        }
    }
}
