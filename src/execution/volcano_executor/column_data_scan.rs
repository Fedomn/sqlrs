use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalColumnDataScan};

#[derive(new)]
pub struct ColumnDataScan {
    pub(crate) plan: PhysicalColumnDataScan,
}

impl ColumnDataScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        for batch in self.plan.collection.into_iter() {
            yield batch;
        }
    }
}
