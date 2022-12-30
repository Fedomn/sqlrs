use std::io;

use arrow::error::ArrowError;

use crate::catalog_v2::CatalogError;
use crate::planner_v2::BindError;
use crate::types_v2::TypeError;

pub type FunctionResult<T> = Result<T, FunctionError>;

// TODO: refactor error using https://docs.rs/snafu/latest/snafu/
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
    #[error("Cast error: {0}")]
    CastError(String),
    #[error("Comparison error: {0}")]
    ComparisonError(String),
    #[error("Conjunction error: {0}")]
    ConjunctionError(String),
    #[error("io error")]
    IoError(#[from] io::Error),
}

impl From<BindError> for FunctionError {
    fn from(e: BindError) -> Self {
        FunctionError::InternalError(e.to_string())
    }
}
