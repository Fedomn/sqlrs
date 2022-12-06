use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalTableScan, SchemaUtil};
use crate::storage_v2::LocalStorage;

#[derive(new)]
pub struct TableScan {
    pub(crate) plan: PhysicalTableScan,
}

impl TableScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let schema = SchemaUtil::new_schema_ref(&self.plan.names, &self.plan.returned_types);

        let table = self.plan.bind_table;
        let mut local_storage_reader = LocalStorage::create_reader(&table.storage);
        let client_context = context.clone_client_context();
        while let Some(batch) = local_storage_reader.next_batch(client_context.clone()) {
            let columns = batch.columns().to_vec();
            yield RecordBatch::try_new(schema.clone(), columns)?
        }
    }
}
