mod scalar_function_catalog_entry;
mod schema_catalog_entry;
mod table_catalog_entry;
mod table_function_catalog_entry;

use derive_new::new;
pub use scalar_function_catalog_entry::*;
pub use schema_catalog_entry::*;
pub use table_catalog_entry::*;
pub use table_function_catalog_entry::*;

#[derive(Clone, Debug)]
pub enum CatalogEntry {
    SchemaCatalogEntry(SchemaCatalogEntry),
    TableCatalogEntry(TableCatalogEntry),
    TableFunctionCatalogEntry(TableFunctionCatalogEntry),
    ScalarFunctionCatalogEntry(ScalarFunctionCatalogEntry),
}

impl CatalogEntry {
    pub fn default_schema_catalog_entry(oid: usize, schema: String) -> Self {
        Self::SchemaCatalogEntry(SchemaCatalogEntry::new(oid, schema))
    }
}

#[allow(dead_code)]
#[derive(new, Clone, Debug)]
pub struct CatalogEntryBase {
    /// The object identifier of the entry
    pub(crate) oid: usize,
    /// The name of the entry
    pub(crate) name: String,
}
