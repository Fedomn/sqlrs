use std::collections::HashMap;

use super::{CatalogEntry, CatalogError};

/// The Catalog Set stores (key, value) map of a set of CatalogEntries
#[derive(Clone, Debug, Default)]
pub struct CatalogSet {
    /// The set of catalog entries, entry index to entry
    entries: HashMap<usize, CatalogEntry>,
    /// Mapping of string to catalog entry index
    mapping: HashMap<String, usize>,
    /// The current catalog entry index
    current_entry: usize,
}

impl CatalogSet {
    pub fn create_entry(&mut self, name: String, entry: CatalogEntry) -> Result<(), CatalogError> {
        if self.mapping.get(&name).is_some() {
            return Err(CatalogError::CatalogEntryExists(name));
        }
        self.current_entry += 1;
        self.entries.insert(self.current_entry, entry);
        self.mapping.insert(name, self.current_entry);
        Ok(())
    }

    pub fn get_entry(&self, name: String) -> Result<CatalogEntry, CatalogError> {
        if let Some(index) = self.mapping.get(&name) {
            if let Some(entry) = self.entries.get(index) {
                return Ok(entry.clone());
            }
        }
        Err(CatalogError::CatalogEntryNotExists(name))
    }

    pub fn replace_entry(
        &mut self,
        name: String,
        new_entry: CatalogEntry,
    ) -> Result<(), CatalogError> {
        if let Some(old_entry_index) = self.mapping.get(&name) {
            if self.entries.get(old_entry_index).is_some() {
                self.entries.insert(*old_entry_index, new_entry);
                return Ok(());
            }
        }
        Err(CatalogError::CatalogEntryNotExists(name))
    }
}
