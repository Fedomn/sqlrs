mod expression;
mod statement;
mod table;

use std::collections::HashMap;

use sqlparser::ast::{Ident, Statement};

use crate::catalog::{RootCatalogRef, TableCatalog};

pub struct Binder {
    catalog: RootCatalogRef,
    context: BinderContext,
}

#[derive(Default)]
struct BinderContext {
    /// table_name == table_id
    /// table_id -> table_catalog
    tables: HashMap<String, TableCatalog>,
}

impl Binder {
    pub fn new(catalog: RootCatalogRef) -> Self {
        Self {
            catalog,
            context: BinderContext::default(),
        }
    }

    pub fn bind(&self, stmt: &Statement) -> Result<Vec<Statement>, BindError> {
        match stmt {
            Statement::Query(_) => todo!(),
            _ => Err(BindError::UnsupportedStmt(stmt.to_string())),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum BindError {
    #[error("unsupported statement {0}")]
    UnsupportedStmt(String),
    #[error("invalid table {0}")]
    InvalidTable(String),
    #[error("invalid table name: {0:?}")]
    InvalidTableName(Vec<Ident>),
    #[error("invalid column {0}")]
    InvalidColumn(String),
}
