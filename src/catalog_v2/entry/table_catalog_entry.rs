use std::collections::HashMap;

use derive_new::new;

use super::CatalogEntryBase;
use crate::types_v2::LogicalType;

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct TableCatalogEntry {
    pub(crate) base: CatalogEntryBase,
    pub(crate) schema_base: CatalogEntryBase,
    pub(crate) storage: DataTable,
    /// A list of columns that are part of this table
    pub(crate) columns: Vec<ColumnDefinition>,
    /// A map of column name to column index
    pub(crate) name_map: HashMap<String, usize>,
}

impl TableCatalogEntry {
    pub fn new(
        oid: usize,
        table: String,
        schema_base: CatalogEntryBase,
        storage: DataTable,
    ) -> Self {
        let mut name_map = HashMap::new();
        let mut columns = vec![];
        storage
            .column_definitions
            .iter()
            .enumerate()
            .for_each(|(idx, col)| {
                columns.push(col.clone());
                name_map.insert(col.name.clone(), idx);
            });
        Self {
            base: CatalogEntryBase::new(oid, table),
            schema_base,
            storage,
            columns,
            name_map,
        }
    }
}

/// DataTable represents a physical table on disk
#[derive(new, Clone, Debug, PartialEq, Eq, Hash)]
pub struct DataTable {
    /// The table info
    pub(crate) info: DataTableInfo,
    /// The set of physical columns stored by this DataTable
    pub(crate) column_definitions: Vec<ColumnDefinition>,
}

#[derive(new, Clone, Debug, PartialEq, Eq, Hash)]
pub struct DataTableInfo {
    /// schema of the table
    pub(crate) schema: String,
    /// name of the table
    pub(crate) table: String,
}

/// A column of a table
#[derive(new, Clone, Debug, PartialEq, Eq, Hash)]
pub struct ColumnDefinition {
    /// The name of the entry
    pub(crate) name: String,
    /// The type of the column
    pub(crate) ty: LogicalType,
}
