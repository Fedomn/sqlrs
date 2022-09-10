use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use super::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog, TableCatalog, TableId};

pub struct InMemoryStorage {
    catalog: Mutex<RootCatalog>,
    tables: Mutex<HashMap<TableId, InMemoryTable>>,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        InMemoryStorage::new()
    }
}

impl InMemoryStorage {
    pub fn new() -> Self {
        InMemoryStorage {
            catalog: Mutex::new(RootCatalog::new()),
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl Storage for InMemoryStorage {
    type TableType = InMemoryTable;

    fn create_csv_table(&self, _id: String, _filepath: String) -> Result<(), StorageError> {
        unreachable!("memory storage does not support create csv table")
    }

    fn create_mem_table(&self, id: String, data: Vec<RecordBatch>) -> Result<(), StorageError> {
        let table = InMemoryTable::new(id.clone(), data)?;
        self.catalog
            .lock()
            .unwrap()
            .tables
            .insert(id.clone(), table.catalog.clone());
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    fn get_table(&self, id: String) -> Result<Self::TableType, StorageError> {
        self.tables
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(StorageError::TableNotFound(id))
    }

    fn get_catalog(&self) -> RootCatalog {
        self.catalog.lock().unwrap().clone()
    }

    fn show_tables(&self) -> Result<RecordBatch, StorageError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("columns", DataType::Utf8, false),
        ]));
        let mut ids = Vec::new();
        let mut columns = Vec::new();
        for (id, table) in self.tables.lock().unwrap().iter() {
            ids.push(id.clone());
            columns.push(format!("{:?}", table.catalog.get_all_columns()));
        }
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(columns)),
            ],
        )?;

        Ok(batch)
    }
}

#[derive(Clone)]
pub struct InMemoryTable {
    _id: TableId,
    catalog: TableCatalog,
    data: Vec<RecordBatch>,
}

impl InMemoryTable {
    pub fn new(id: TableId, data: Vec<RecordBatch>) -> Result<Self, StorageError> {
        let catalog = Self::infer_catalog(id.clone(), data.first().cloned());
        Ok(Self {
            _id: id,
            data,
            catalog,
        })
    }

    fn infer_catalog(id: String, batch: Option<RecordBatch>) -> TableCatalog {
        let mut columns = BTreeMap::new();
        let mut column_ids = Vec::new();
        if let Some(batch) = batch {
            for f in batch.schema().fields().iter() {
                let field_name = f.name().to_string();
                column_ids.push(field_name.clone());
                columns.insert(
                    field_name.clone(),
                    ColumnCatalog {
                        table_id: id.clone(),
                        column_id: field_name.clone(),
                        desc: ColumnDesc {
                            name: field_name,
                            data_type: f.data_type().clone(),
                        },
                        nullable: f.is_nullable(),
                    },
                );
            }
        }
        TableCatalog {
            id: id.clone(),
            name: id,
            columns,
            column_ids,
        }
    }
}

impl Table for InMemoryTable {
    type TransactionType = InMemoryTransaction;

    fn read(
        &self,
        _bounds: Bounds,
        _projection: Projections,
    ) -> Result<Self::TransactionType, StorageError> {
        InMemoryTransaction::start(self)
    }
}

pub struct InMemoryTransaction {
    batch_cursor: usize,
    data: Vec<RecordBatch>,
}

impl InMemoryTransaction {
    pub fn start(table: &InMemoryTable) -> Result<Self, StorageError> {
        Ok(Self {
            batch_cursor: 0,
            data: table.data.clone(),
        })
    }
}

impl Transaction for InMemoryTransaction {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError> {
        self.data
            .get(self.batch_cursor)
            .map(|batch| {
                self.batch_cursor += 1;
                Ok(batch.clone())
            })
            .transpose()
    }
}

#[cfg(test)]
mod storage_test {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_in_memory_storage_works_with_empty_data() -> Result<(), StorageError> {
        let id = "test".to_string();
        let storage = InMemoryStorage::new();
        storage.create_mem_table(id.clone(), vec![])?;

        let catalog = storage.get_catalog();
        let table_catalog = catalog.get_table_by_name(id.as_str());
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_all_columns().is_empty());

        let table = storage.get_table(id)?;
        let mut tx = table.read(None, None)?;
        let batch = tx.next_batch()?;
        assert!(batch.is_none());

        Ok(())
    }

    fn build_record_batch() -> Result<Vec<RecordBatch>, StorageError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
            ],
        )?;
        Ok(vec![batch])
    }

    #[test]
    fn test_in_memory_storage_works_with_data() -> Result<(), StorageError> {
        let id = "test".to_string();
        let storage = InMemoryStorage::new();
        storage.create_mem_table(id.clone(), build_record_batch()?)?;

        let catalog = storage.get_catalog();
        let table_catalog = catalog.get_table_by_name(id.as_str());
        assert!(table_catalog.is_some());
        assert!(table_catalog.unwrap().get_column_by_name("a").is_some());

        let table = storage.get_table(id)?;
        let mut tx = table.read(None, None)?;
        let batch = tx.next_batch()?;
        println!("{:?}", batch);
        assert!(batch.is_some());
        assert_eq!(batch.unwrap().num_rows(), 3);

        Ok(())
    }
}
