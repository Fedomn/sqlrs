use std::sync::Arc;

use super::entry::{CatalogEntry, DataTable};
use super::{
    CatalogError, CatalogSet, ScalarFunctionCatalogEntry, TableCatalogEntry,
    TableFunctionCatalogEntry,
};
use crate::common::{CreateScalarFunctionInfo, CreateTableFunctionInfo};
use crate::main_entry::ClientContext;

/// The Catalog object represents the catalog of the database.
#[derive(Clone, Debug, Default)]
pub struct Catalog {
    /// The catalog set holding the schemas
    schemas: CatalogSet,
    /// The catalog version, incremented whenever anything changes in the catalog
    catalog_version: usize,
}

impl Catalog {
    pub fn create_schema(&mut self, name: String) -> Result<(), CatalogError> {
        self.catalog_version += 1;
        let entry = CatalogEntry::default_schema_catalog_entry(self.catalog_version, name.clone());
        self.schemas.create_entry(name, entry)
    }

    pub fn create_table(
        client_context: Arc<ClientContext>,
        schema: String,
        table: String,
        data_table: DataTable,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(mut entry) =
            catalog.schemas.get_entry(schema.clone())?
        {
            catalog.catalog_version += 1;
            entry.create_table(catalog.catalog_version, table, data_table)?;
            catalog
                .schemas
                .replace_entry(schema, CatalogEntry::SchemaCatalogEntry(entry))?;
            return Ok(());
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn get_table(
        client_context: Arc<ClientContext>,
        schema: String,
        table: String,
    ) -> Result<TableCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_table(table);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn create_table_function(
        client_context: Arc<ClientContext>,
        info: CreateTableFunctionInfo,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(mut entry) =
            catalog.schemas.get_entry(info.base.schema.clone())?
        {
            catalog.catalog_version += 1;
            let schema = info.base.schema.clone();
            entry.create_table_function(catalog.catalog_version, info)?;
            catalog
                .schemas
                .replace_entry(schema, CatalogEntry::SchemaCatalogEntry(entry))?;
            return Ok(());
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn scan_entries<F>(
        client_context: Arc<ClientContext>,
        schema: String,
        callback: &F,
    ) -> Result<Vec<CatalogEntry>, CatalogError>
    where
        F: Fn(&CatalogEntry) -> bool,
    {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return Ok(entry.scan_entries(callback));
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn get_table_function(
        client_context: Arc<ClientContext>,
        schema: String,
        table_function: String,
    ) -> Result<TableFunctionCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_table_function(table_function);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }

    pub fn create_scalar_function(
        client_context: Arc<ClientContext>,
        info: CreateScalarFunctionInfo,
    ) -> Result<(), CatalogError> {
        let mut catalog = match client_context.db.catalog.try_write() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        let version = catalog.catalog_version;
        let entry = catalog.schemas.get_mut_entry(info.base.schema.clone())?;

        if let CatalogEntry::SchemaCatalogEntry(mut_entry) = entry {
            mut_entry.create_scalar_function(version + 1, info)?;
            catalog.catalog_version += 1;
            Ok(())
        } else {
            Err(CatalogError::CatalogEntryTypeNotMatch)
        }
    }

    pub fn get_scalar_function(
        client_context: Arc<ClientContext>,
        schema: String,
        scalar_function: String,
    ) -> Result<ScalarFunctionCatalogEntry, CatalogError> {
        let catalog = match client_context.db.catalog.try_read() {
            Ok(c) => c,
            Err(_) => return Err(CatalogError::CatalogLockedError),
        };
        if let CatalogEntry::SchemaCatalogEntry(entry) = catalog.schemas.get_entry(schema)? {
            return entry.get_scalar_function(scalar_function);
        }
        Err(CatalogError::CatalogEntryTypeNotMatch)
    }
}
