use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;

use crate::catalog_v2::DataTable;
use crate::main_entry::ClientContext;

/// Used as in-memory storage
#[derive(Default)]
pub struct LocalStorage {
    table_manager: LocalTableManager,
}

impl LocalStorage {
    fn append_internal(&mut self, table: &DataTable, batch: RecordBatch) {
        self.table_manager.init_storage(table);
        self.table_manager.append(table, batch);
    }

    pub fn append(client_context: Arc<ClientContext>, table: &DataTable, batch: RecordBatch) {
        let mut storage = client_context.db.storage.try_write().unwrap();
        storage.append_internal(table, batch);
    }

    pub fn create_reader(table: &DataTable) -> LocalStorageReader {
        LocalStorageReader::new(table.clone())
    }
}

#[derive(new)]
pub struct LocalStorageReader {
    table: DataTable,
    #[new(default)]
    current_batch_cursor: usize,
}

impl LocalStorageReader {
    pub fn next_batch(&mut self, client_context: Arc<ClientContext>) -> Option<RecordBatch> {
        let storage = client_context.db.storage.try_read().unwrap();
        let batch = storage
            .table_manager
            .fetch_table_batch(&self.table, self.current_batch_cursor);
        self.current_batch_cursor += 1;
        batch
    }
}

#[derive(Default)]
pub struct LocalTableManager {
    table_storage: HashMap<DataTable, LocalTableStorage>,
}

impl LocalTableManager {
    pub fn init_storage(&mut self, table: &DataTable) {
        if !self.table_storage.contains_key(table) {
            let storage = LocalTableStorage::new(table.clone());
            self.table_storage.insert(table.clone(), storage);
        }
    }

    fn append(&mut self, table: &DataTable, batch: RecordBatch) {
        self.table_storage.get_mut(table).unwrap().append(batch);
    }

    pub fn fetch_table_batch(&self, table: &DataTable, batch_idx: usize) -> Option<RecordBatch> {
        self.table_storage
            .get(table)
            .unwrap()
            .fetch_batch(batch_idx)
    }
}

pub struct LocalTableStorage {
    _table: DataTable,
    data: Vec<RecordBatch>,
}

impl LocalTableStorage {
    pub fn new(table: DataTable) -> Self {
        Self {
            _table: table,
            data: vec![],
        }
    }

    fn append(&mut self, batch: RecordBatch) {
        self.data.push(batch);
    }

    fn fetch_batch(&self, batch_idx: usize) -> Option<RecordBatch> {
        if batch_idx >= self.data.len() {
            None
        } else {
            Some(self.data[batch_idx].clone())
        }
    }
}
