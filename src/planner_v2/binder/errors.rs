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
        crate::catalog_v2::CatalogError,
    ),
}