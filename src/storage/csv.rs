use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog, TableCatalog, TableId};
use arrow::{
    csv::{reader, Reader},
    datatypes::{Schema, SchemaRef},
    record_batch::RecordBatch,
};

use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    sync::{Arc, Mutex},
    usize,
};

use super::{Storage, StorageError, Table, Transaction};

pub struct CsvStorage {
    catalog: Mutex<RootCatalog>,
    tables: Mutex<HashMap<TableId, CsvTable>>,
}

impl CsvStorage {
    pub fn new() -> Self {
        CsvStorage {
            catalog: Mutex::new(RootCatalog::new()),
            tables: Mutex::new(HashMap::new()),
        }
    }
}

impl Storage for CsvStorage {
    type TableType = CsvTable;

    fn create_table(&self, id: String, filepath: String) -> Result<(), StorageError> {
        let table = CsvTable::new(id.clone(), filepath, CsvConfig::default())?;
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
}

#[derive(Clone)]
pub struct CsvConfig {
    has_header: bool,
    delimiter: u8,
    infer_schema_max_read_records: Option<usize>,
    batch_size: usize,
    projection: Option<Vec<usize>>,
    datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            infer_schema_max_read_records: Some(10),
            batch_size: 1024,
            projection: None,
            datetime_format: None,
        }
    }
}

#[derive(Clone)]
pub struct CsvTable {
    id: TableId,
    arrow_schema: SchemaRef,
    arrow_csv_cfg: CsvConfig,
    filepath: String,
    catalog: TableCatalog,
}

impl CsvTable {
    pub fn new(id: String, filepath: String, cfg: CsvConfig) -> Result<Self, StorageError> {
        let schema = Self::infer_arrow_schema(filepath.clone(), &cfg)?;
        let catalog = Self::infer_catalog(id.clone(), id.clone(), &schema);
        Ok(Self {
            id,
            arrow_schema: Arc::new(schema),
            arrow_csv_cfg: cfg,
            filepath,
            catalog,
        })
    }

    fn infer_arrow_schema(filepath: String, cfg: &CsvConfig) -> Result<Schema, StorageError> {
        let mut file = File::open(filepath)?;
        let (schema, _) = reader::infer_reader_schema(
            &mut file,
            cfg.delimiter,
            cfg.infer_schema_max_read_records,
            cfg.has_header,
        )?;
        Ok(schema)
    }

    fn infer_catalog(id: String, name: String, schema: &Schema) -> TableCatalog {
        let mut columns = BTreeMap::new();
        for f in schema.fields().iter() {
            let field_name = f.name().to_string();
            columns.insert(
                field_name.clone(),
                ColumnCatalog {
                    id: field_name.clone(),
                    desc: ColumnDesc {
                        name: field_name,
                        data_type: f.data_type().clone(),
                    },
                },
            );
        }
        TableCatalog { id, name, columns }
    }
}

impl Table for CsvTable {
    type TransactionType = CsvTransaction;

    fn read(&self) -> Result<Self::TransactionType, StorageError> {
        CsvTransaction::start(self)
    }
}

pub struct CsvTransaction {
    reader: Reader<File>,
}

impl CsvTransaction {
    pub fn start(table: &CsvTable) -> Result<Self, StorageError> {
        Ok(Self {
            reader: Self::create_reader(
                table.filepath.clone(),
                table.arrow_schema.clone(),
                &table.arrow_csv_cfg,
            )?,
        })
    }

    fn create_reader(
        filepath: String,
        schema: SchemaRef,
        cfg: &CsvConfig,
    ) -> Result<Reader<File>, StorageError> {
        let file = File::open(filepath)?;
        let reader = Reader::new(
            file,
            schema,
            cfg.has_header,
            Some(cfg.delimiter),
            cfg.batch_size,
            None,
            cfg.projection.clone(),
            cfg.datetime_format.clone(),
        );
        Ok(reader)
    }
}

impl Transaction for CsvTransaction {
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, StorageError> {
        let batch = self.reader.next().transpose()?;
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_csv_storage_works() -> Result<(), StorageError> {
        let id = "test".to_string();
        let filepath = "./tests/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_table(id.clone(), filepath)?;
        let table = storage.get_table(id)?;
        let mut tx = table.read()?;
        let batch = tx.next_batch()?;
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 4);
        Ok(())
    }
}
