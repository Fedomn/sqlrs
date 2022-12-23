use derive_new::new;

use super::CatalogEntryBase;
use crate::function::ScalarFunction;

#[derive(new, Clone, Debug)]
pub struct ScalarFunctionCatalogEntry {
    #[allow(dead_code)]
    pub(crate) base: CatalogEntryBase,
    #[allow(dead_code)]
    pub(crate) functions: Vec<ScalarFunction>,
}
