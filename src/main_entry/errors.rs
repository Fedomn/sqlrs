use arrow::error::ArrowError;
use sqlparser::parser::ParserError;

use crate::execution::ExecutorError;
use crate::planner_v2::PlannerError;

#[derive(thiserror::Error, Debug)]
pub enum DatabaseError {
    #[error("parse error: {0}")]
    ParserError(
        #[source]
        #[from]
        ParserError,
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
    #[error("Internal error: {0}")]
    InternalError(String),
}
