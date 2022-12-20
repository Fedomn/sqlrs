use arrow::error::ArrowError;

use crate::catalog_v2::CatalogError;
use crate::types_v2::TypeError;

#[derive(thiserror::Error, Debug)]
pub enum FunctionError {
    #[error("catalog error: {0}")]
    CatalogError(
        #[from]
        #[source]
        CatalogError,
    ),
    #[error("type error: {0}")]
    TypeError(
        #[from]
        #[source]
        TypeError,
    ),
    #[error("arrow error: {0}")]
    ArrowError(
        #[from]
        #[source]
        ArrowError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}
