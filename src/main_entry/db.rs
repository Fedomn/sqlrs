use std::sync::{Arc, RwLock};

use super::DatabaseError;
use crate::catalog_v2::{Catalog, CatalogError, DEFAULT_SCHEMA};
use crate::storage_v2::LocalStorage;

#[derive(Default)]
pub struct DatabaseInstance {
    pub(crate) storage: RwLock<LocalStorage>,
    pub(crate) catalog: Arc<RwLock<Catalog>>,
}

impl DatabaseInstance {
    pub fn initialize(self: &Arc<Self>) -> Result<(), DatabaseError> {
        // Create the default schema: main
        self.init_default_schema()?;
        Ok(())
    }

    fn init_default_schema(self: &Arc<Self>) -> Result<(), DatabaseError> {
        let mut catalog = match self.catalog.try_write() {
            Ok(c) => c,
            Err(_) => {
                return Err(DatabaseError::CatalogError(
                    CatalogError::CatalogLockedError,
                ))
            }
        };
        catalog.create_schema(DEFAULT_SCHEMA.to_string()).unwrap();
        Ok(())
    }
}
