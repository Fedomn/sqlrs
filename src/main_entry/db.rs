use std::sync::{Arc, RwLock};

use crate::catalog_v2::{Catalog, DEFAULT_SCHEMA};
use crate::storage_v2::LocalStorage;

pub struct DatabaseInstance {
    pub(crate) storage: RwLock<LocalStorage>,
    pub(crate) catalog: Arc<RwLock<Catalog>>,
}

impl Default for DatabaseInstance {
    fn default() -> Self {
        let mut catalog = Catalog::default();
        catalog.create_schema(DEFAULT_SCHEMA.to_string()).unwrap();
        Self {
            storage: RwLock::new(LocalStorage::default()),
            catalog: Arc::new(RwLock::new(catalog)),
        }
    }
}
