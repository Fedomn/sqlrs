#[derive(thiserror::Error, Debug)]
pub enum CatalogError {
    #[error("CatalogEntry: {0} already exists")]
    CatalogEntryExists(String),
    #[error("CatalogEntry: {0} not exists")]
    CatalogEntryNotExists(String),
    #[error("CatalogEntry type not match")]
    CatalogEntryTypeNotMatch,
    #[error("Catalog locked, please retry")]
    CatalogLockedError,
}
