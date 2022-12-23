use super::table_catalog_entry::{DataTable, TableCatalogEntry};
use super::{
    CatalogEntry, CatalogEntryBase, ScalarFunctionCatalogEntry, TableFunctionCatalogEntry,
};
use crate::catalog_v2::{CatalogError, CatalogSet};
use crate::common::{CreateScalarFunctionInfo, CreateTableFunctionInfo};

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct SchemaCatalogEntry {
    base: CatalogEntryBase,
    tables: CatalogSet,
    functions: CatalogSet,
}

impl SchemaCatalogEntry {
    pub fn new(oid: usize, schema: String) -> Self {
        Self {
            base: CatalogEntryBase::new(oid, schema),
            tables: CatalogSet::default(),
            functions: CatalogSet::default(),
        }
    }

    pub fn create_table(
        &mut self,
        oid: usize,
        table: String,
        storage: DataTable,
    ) -> Result<(), CatalogError> {
        let entry = CatalogEntry::TableCatalogEntry(TableCatalogEntry::new(
            oid,
            table.clone(),
            self.base.clone(),
            storage,
        ));
        self.tables.create_entry(table, entry)?;
        Ok(())
    }

    pub fn get_table(&self, table: String) -> Result<TableCatalogEntry, CatalogError> {
        match self.tables.get_entry(table.clone())? {
            CatalogEntry::TableCatalogEntry(e) => Ok(e),
            _ => Err(CatalogError::CatalogEntryNotExists(table)),
        }
    }

    pub fn create_table_function(
        &mut self,
        oid: usize,
        info: CreateTableFunctionInfo,
    ) -> Result<(), CatalogError> {
        let entry = TableFunctionCatalogEntry::new(
            CatalogEntryBase::new(oid, info.name.clone()),
            info.functions,
        );
        let entry = CatalogEntry::TableFunctionCatalogEntry(entry);
        self.functions.create_entry(info.name, entry)?;
        Ok(())
    }

    pub fn get_table_function(
        &self,
        table_function: String,
    ) -> Result<TableFunctionCatalogEntry, CatalogError> {
        match self.functions.get_entry(table_function.clone())? {
            CatalogEntry::TableFunctionCatalogEntry(e) => Ok(e),
            _ => Err(CatalogError::CatalogEntryNotExists(table_function)),
        }
    }

    pub fn scan_entries<F>(&self, callback: &F) -> Vec<CatalogEntry>
    where
        F: Fn(&CatalogEntry) -> bool,
    {
        let mut result = vec![];
        result.extend(self.tables.scan_entries(callback));
        result.extend(self.functions.scan_entries(callback));
        result
    }

    pub fn create_scalar_function(
        &mut self,
        oid: usize,
        info: CreateScalarFunctionInfo,
    ) -> Result<(), CatalogError> {
        let entry = ScalarFunctionCatalogEntry::new(
            CatalogEntryBase::new(oid, info.name.clone()),
            info.functions,
        );
        let entry = CatalogEntry::ScalarFunctionCatalogEntry(entry);
        self.functions.create_entry(info.name, entry)?;
        Ok(())
    }

    pub fn get_scalar_function(
        &self,
        scalar_function: String,
    ) -> Result<ScalarFunctionCatalogEntry, CatalogError> {
        match self.functions.get_entry(scalar_function.clone())? {
            CatalogEntry::ScalarFunctionCatalogEntry(e) => Ok(e),
            _ => Err(CatalogError::CatalogEntryNotExists(scalar_function)),
        }
    }
}
