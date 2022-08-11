use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field};

pub type RootCatalogRef = Arc<RootCatalog>;

#[derive(Debug, Clone)]
pub struct RootCatalog {
    pub tables: HashMap<TableId, TableCatalog>,
}

impl Default for RootCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl RootCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn get_table_by_name(&self, name: &str) -> Option<TableCatalog> {
        self.tables.get(name).cloned()
    }
}

/// use table name as id for simplicity
pub type TableId = String;

#[derive(Clone, PartialEq)]
pub struct TableCatalog {
    pub id: TableId,
    pub name: String,
    /// column_ids to keep the order of inferred columns
    pub column_ids: Vec<ColumnId>,
    pub columns: BTreeMap<ColumnId, ColumnCatalog>,
}

impl TableCatalog {
    pub fn get_column_by_name(&self, name: &str) -> Option<ColumnCatalog> {
        self.columns.get(name).cloned()
    }

    pub fn get_all_columns(&self) -> Vec<ColumnCatalog> {
        self.column_ids
            .iter()
            .map(|id| self.columns.get(id).cloned().unwrap())
            .collect()
    }
}

/// use column name as id for simplicity
pub type ColumnId = String;

#[derive(Clone, PartialEq)]
pub struct ColumnCatalog {
    pub table_id: TableId,
    pub column_id: ColumnId,
    pub nullable: bool,
    pub desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn to_arrow_field(&self) -> Field {
        Field::new(
            self.desc.name.clone().as_str(),
            self.desc.data_type.clone(),
            self.nullable,
        )
    }

    pub fn from_arrow_field(table_id: &str, field: &Field) -> Self {
        Self {
            table_id: table_id.to_string(),
            column_id: field.name().to_string(),
            nullable: field.is_nullable(),
            desc: ColumnDesc {
                name: field.name().to_string(),
                data_type: field.data_type().clone(),
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: DataType,
}

impl fmt::Debug for ColumnCatalog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}.{}:{:?}",
            self.table_id, self.column_id, self.desc.data_type
        )
    }
}

impl fmt::Debug for TableCatalog {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            r#"{} {{
    columns: {:?}
}}"#,
            self.id,
            self.get_all_columns()
        )
    }
}
