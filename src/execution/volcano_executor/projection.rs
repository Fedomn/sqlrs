use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{
    BoxedExecutor, ExecutionContext, ExecutorError, ExpressionExecutor, PhysicalProjection,
    SchemaUtil,
};

#[derive(new)]
pub struct Projection {
    pub(crate) plan: PhysicalProjection,
    pub(crate) child: BoxedExecutor,
}

impl Projection {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let exprs = self.plan.base.expressioins;
        let schema = SchemaUtil::new_schema_ref_from_exprs(&exprs);

        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let columns = ExpressionExecutor::execute(&exprs, &batch)?;
            yield RecordBatch::try_new(schema.clone(), columns)?;
        }
    }
}
