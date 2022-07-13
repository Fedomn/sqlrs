use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use super::*;
use crate::optimizer::PhysicalTableScan;
use crate::storage::{Storage, Table, Transaction};

pub struct TableScanExecutor<S: Storage> {
    pub plan: PhysicalTableScan,
    pub storage: Arc<S>,
}

impl<S: Storage> TableScanExecutor<S> {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let table_id = self.plan.logical().table_id();
        let table = self.storage.get_table(table_id)?;
        let mut tx = table.read()?;
        loop {
            match tx.next_batch() {
                Ok(batch) => {
                    if let Some(batch) = batch {
                        yield batch;
                    } else {
                        break;
                    }
                }
                Err(err) => return Err(err.into()),
            }
        }
    }
}
