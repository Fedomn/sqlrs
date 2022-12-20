use derive_new::new;

use super::CatalogEntryBase;
use crate::function::TableFunction;

#[derive(new, Clone, Debug)]
pub struct TableFunctionCatalogEntry {
    #[allow(dead_code)]
    pub(crate) base: CatalogEntryBase,
    #[allow(dead_code)]
    pub(crate) functions: Vec<TableFunction>,
}
