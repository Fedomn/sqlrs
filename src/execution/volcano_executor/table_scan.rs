use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalTableScan, SchemaUtil};
use crate::function::{
    GlobalTableFunctionState, SeqTableScanInitInput, TableFunctionInitInput, TableFunctionInput,
};

#[derive(new)]
pub struct TableScan {
    pub(crate) plan: PhysicalTableScan,
}

impl TableScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let schema = SchemaUtil::new_schema_ref(&self.plan.names, &self.plan.returned_types);

        let bind_data = self.plan.bind_data;

        let table_scan_func = self.plan.function.function;
        let global_state = if let Some(init_global_func) = self.plan.function.init_global {
            let seq_table_scan_init_input = TableFunctionInitInput::SeqTableScanInitInput(
                Box::new(SeqTableScanInitInput::new(bind_data.clone())),
            );
            init_global_func(context.clone_client_context(), seq_table_scan_init_input)?
        } else {
            GlobalTableFunctionState::None
        };

        let mut tabel_scan_input = TableFunctionInput::new(bind_data, global_state);
        while let Some(batch) =
            table_scan_func(context.clone_client_context(), &mut tabel_scan_input)?
        {
            let columns = batch.columns().to_vec();
            yield RecordBatch::try_new(schema.clone(), columns)?
        }
    }
}
