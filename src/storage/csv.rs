use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::sync::{Arc, Mutex};
use std::usize;

use arrow::array::StringArray;
use arrow::csv::{reader, Reader};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::{Bounds, Projections, Storage, StorageError, Table, Transaction};
use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog, TableCatalog, TableId};

pub struct CsvStorage {
    catalog: Mutex<RootCatalog>,
    tables: Mutex<HashMap<TableId, CsvTable>>,
}

impl Default for CsvStorage {
    fn default() -> Self {
        CsvStorage::new()
    }
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

    fn create_csv_table(&self, id: String, filepath: String) -> Result<(), StorageError> {
        let table = CsvTable::new(id.clone(), filepath, CsvConfig::default())?;
        self.catalog
            .lock()
            .unwrap()
            .tables
            .insert(id.clone(), table.catalog.clone());
        self.tables.lock().unwrap().insert(id, table);
        Ok(())
    }

    fn create_mem_table(&self, _id: String, _data: Vec<RecordBatch>) -> Result<(), StorageError> {
        unreachable!("csv storage does not support create memory table")
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
pub struct CsvConfig {
    has_header: bool,
    delimiter: u8,
    infer_schema_max_read_records: Option<usize>,
    batch_size: usize,
    datetime_format: Option<String>,
}

impl Default for CsvConfig {
    fn default() -> Self {
        Self {
            has_header: true,
            delimiter: b',',
            infer_schema_max_read_records: Some(10),
            batch_size: 1024,
            datetime_format: None,
        }
    }
}

#[derive(Clone)]
pub struct CsvTable {
    _id: TableId,
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
            _id: id,
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
        let mut column_ids = Vec::new();
        for f in schema.fields().iter() {
            let field_name = f.name().to_lowercase();
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
        TableCatalog {
            id,
            name,
            columns,
            column_ids,
        }
    }
}

impl Table for CsvTable {
    type TransactionType = CsvTransaction;

    fn read(
        &self,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self::TransactionType, StorageError> {
        CsvTransaction::start(self, bounds, projection)
    }
}

pub struct CsvTransaction {
    reader: Reader<File>,
}

impl CsvTransaction {
    /// The bounds is applied to the whole data batches, not per batch.
    pub fn start(
        table: &CsvTable,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Self, StorageError> {
        Ok(Self {
            reader: Self::create_reader(
                table.filepath.clone(),
                table.arrow_schema.clone(),
                &table.arrow_csv_cfg,
                bounds,
                projection,
            )?,
        })
    }

    fn create_reader(
        filepath: String,
        schema: SchemaRef,
        cfg: &CsvConfig,
        bounds: Bounds,
        projection: Projections,
    ) -> Result<Reader<File>, StorageError> {
        let file = File::open(filepath)?;
        // convert bounds into csv bounds concept: (min line, max line)
        let new_bounds = bounds.map(|(offset, limit)| {
            // set in PushLimitIntoTableScan
            if limit == usize::MAX {
                (offset, limit)
            } else {
                (offset, offset + limit + 1)
            }
        });
        let reader = Reader::new(
            file,
            schema,
            cfg.has_header,
            Some(cfg.delimiter),
            cfg.batch_size,
            new_bounds,
            projection,
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
    use arrow::array::Int64Array;

    use super::*;

    #[test]
    fn test_csv_storage_works() -> Result<(), StorageError> {
        let id = "test".to_string();
        let filepath = "./tests/csv/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_csv_table(id.clone(), filepath)?;
        let table = storage.get_table(id)?;
        let mut tx = table.read(None, None)?;

        let batch = tx.next_batch()?;
        assert!(batch.is_some());
        let batch = batch.unwrap();
        assert_eq!(batch.num_rows(), 4);

        Ok(())
    }

    fn extract_id_column(batch: &RecordBatch) -> &Int64Array {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
    }

    #[test]
    fn test_csv_bounds_works() -> Result<(), StorageError> {
        let id = "test".to_string();
        let filepath = "./tests/csv/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_csv_table(id.clone(), filepath)?;
        let table = storage.get_table(id)?;

        // offset 0, limit 0
        let mut tx = table.read(Some((0, 0)), None)?;
        let batch = tx.next_batch()?;
        assert!(batch.is_none());

        // offset 1, limit 0
        let mut tx = table.read(Some((1, 0)), None)?;
        let batch = tx.next_batch()?;
        assert!(batch.is_none());

        // offset 0, limit 1
        let mut tx = table.read(Some((0, 1)), None)?;
        let batch = tx.next_batch()?.unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(extract_id_column(&batch), &Int64Array::from(vec![1]));

        // offset 1, limit 2
        let mut tx = table.read(Some((1, 2)), None)?;
        let batch = tx.next_batch()?.unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(extract_id_column(&batch), &Int64Array::from(vec![2, 3]));

        Ok(())
    }

    #[test]
    fn test_csv_projections_works() -> Result<(), StorageError> {
        let id = "test".to_string();
        let filepath = "./tests/csv/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_csv_table(id.clone(), filepath)?;
        let table = storage.get_table(id)?;

        let mut tx = table.read(None, Some(vec![3]))?;
        let batch = tx.next_batch()?.unwrap();

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(column, &StringArray::from(vec!["CA", "CO", "CO", ""]));

        Ok(())
    }

    #[test]
    fn test_csv_projections_with_bounds_works() -> Result<(), StorageError> {
        let id = "test".to_string();
        let filepath = "./tests/csv/employee.csv".to_string();
        let storage = CsvStorage::new();
        storage.create_csv_table(id.clone(), filepath)?;
        let table = storage.get_table(id)?;

        let mut tx = table.read(Some((1, 2)), Some(vec![3]))?;
        let batch = tx.next_batch()?.unwrap();

        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!(column, &StringArray::from(vec!["CO", "CO"]));

        Ok(())
    }
}
