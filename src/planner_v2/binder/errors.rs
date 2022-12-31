use sqlparser::parser::ParserError;

use crate::catalog_v2::CatalogError;
use crate::execution::ExecutorError;
use crate::function::FunctionError;

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("unsupported expr: {0}")]
    UnsupportedExpr(String),
    #[error("unsupported statement: {0}")]
    UnsupportedStmt(String),
    #[error("sqlparser unsupported statement: {0}")]
    SqlParserUnsupportedStmt(String),
    #[error("bind internal error: {0}")]
    Internal(String),
    #[error("{0}")]
    FunctionBindError(String),
    #[error("type error: {0}")]
    TypeError(
        #[from]
        #[source]
        crate::types_v2::TypeError,
    ),
    #[error("catalog error: {0}")]
    CatalogError(
        #[from]
        #[source]
        CatalogError,
    ),
    #[error("function error: {0}")]
    FunctionError(
        #[from]
        #[source]
        FunctionError,
    ),
    #[error("executor error: {0}")]
    ExecutorError(
        #[from]
        #[source]
        ExecutorError,
    ),
    #[error("parse error: {0}")]
    ParserError(
        #[from]
        #[source]
        ParserError,
    ),
}
