use arrow::record_batch::RecordBatch;

use super::*;
use crate::binder::BoundExpr;
pub struct ProjectExecutor {
    pub exprs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            yield batch;
        }
    }
}
