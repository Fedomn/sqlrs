use std::{
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use arrow::datatypes::DataType;

pub type RootCatalogRef = Arc<RootCatalog>;

#[derive(Debug, Clone)]
pub struct RootCatalog {
    pub tables: HashMap<TableId, TableCatalog>,
}

impl RootCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

// use table name as id for simplicity
pub type TableId = String;

#[derive(Debug, Clone)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    pub columns: BTreeMap<ColumnId, ColumnCatalog>,
}

// use column name as id for simplicity
pub type ColumnId = String;

#[derive(Debug, Clone)]
pub struct ColumnCatalog {
    pub id: ColumnId,
    pub desc: ColumnDesc,
}

#[derive(Debug, Clone)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: DataType,
}
