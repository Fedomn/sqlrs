use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalTableScan, SchemaUtil};
use crate::function::TableFunctionInput;

#[derive(new)]
pub struct TableScan {
    pub(crate) plan: PhysicalTableScan,
}

impl TableScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let schema = SchemaUtil::new_schema_ref(&self.plan.names, &self.plan.returned_types);

        let bind_data = self.plan.bind_data;

        let function = self.plan.function;
        let table_scan_func = function.function;
        let mut tabel_scan_input = TableFunctionInput::new(bind_data);

        while let Some(batch) =
            table_scan_func(context.clone_client_context(), &mut tabel_scan_input)?
        {
            let columns = batch.columns().to_vec();
            let try_new = RecordBatch::try_new(schema.clone(), columns)?;
            yield try_new
        }
    }
}
