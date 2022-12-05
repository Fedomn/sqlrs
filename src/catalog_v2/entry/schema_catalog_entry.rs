use super::table_catalog_entry::{DataTable, TableCatalogEntry};
use super::{CatalogEntry, CatalogEntryBase};
use crate::catalog_v2::{CatalogError, CatalogSet};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SchemaCatalogEntry {
    base: CatalogEntryBase,
    tables: CatalogSet,
}

impl SchemaCatalogEntry {
    pub fn new(oid: usize, schema: String) -> Self {
        Self {
            base: CatalogEntryBase::new(oid, schema),
            tables: CatalogSet::default(),
        }
    }

    pub fn create_table(
        &mut self,
        oid: usize,
        table: String,
        storage: DataTable,
    ) -> Result<(), CatalogError> {
        let entry =
            CatalogEntry::TableCatalogEntry(TableCatalogEntry::new(oid, table.clone(), storage));
        self.tables.create_entry(table, entry)?;
        Ok(())
    }

    pub fn get_table(&self, table: String) -> Result<TableCatalogEntry, CatalogError> {
        match self.tables.get_entry(table.clone())? {
            CatalogEntry::TableCatalogEntry(e) => Ok(e),
            _ => Err(CatalogError::CatalogEntryNotExists(table)),
        }
    }
}
