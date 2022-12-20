use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

use crate::catalog_v2::CatalogError;
use crate::execution::ExecutorError;
use crate::function::FunctionError;
use crate::planner_v2::PlannerError;

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("parse error: {0}")]
    ParserError(
        #[source]
        #[from]
        ParserError,
    ),
    #[error("catalog error: {0}")]
    CatalogError(
        #[source]
        #[from]
        CatalogError,
    ),
    #[error("planner error: {0}")]
    PlannerError(
        #[source]
        #[from]
        PlannerError,
    ),
    #[error("executor error: {0}")]
    ExecutorError(
        #[source]
        #[from]
        ExecutorError,
    ),
    #[error("Arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        ArrowError,
    ),
    #[error("Function error: {0}")]
    FunctionError(
        #[source]
        #[from]
        FunctionError,
    ),
    #[error("Internal error: {0}")]
    InternalError(String),
}
