mod expression;
mod statement;
mod table;

use std::collections::HashMap;

use sqlparser::ast::{Ident, Statement};

use crate::catalog::{RootCatalogRef, TableCatalog};

use self::statement::BoundStatement;

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

    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, BindError> {
        match stmt {
            Statement::Query(query) => {
                let bound_select = self.bind_select(query)?;
                Ok(BoundStatement::Select(bound_select))
            }
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

#[cfg(test)]
mod binder_test {
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::datatypes::DataType;

    use crate::{
        catalog::{ColumnCatalog, ColumnDesc, RootCatalog},
        parser::parse,
    };

    use super::*;

    #[test]
    fn test_bind_select_works() {
        let mut catalog = RootCatalog::new();
        let table_id = "t1".to_string();
        let mut columns = BTreeMap::new();
        columns.insert(
            "c1".to_string(),
            ColumnCatalog {
                id: "c1".to_string(),
                desc: ColumnDesc {
                    name: "c1".to_string(),
                    data_type: DataType::Int32,
                },
            },
        );
        columns.insert(
            "c2".to_string(),
            ColumnCatalog {
                id: "c2".to_string(),
                desc: ColumnDesc {
                    name: "c2".to_string(),
                    data_type: DataType::Int32,
                },
            },
        );
        let table_catalog = TableCatalog {
            id: table_id.clone(),
            name: table_id.clone(),
            columns,
        };
        catalog.tables.insert(table_id, table_catalog);
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1, c2 from t1").unwrap();

        let res = binder.bind(&stats[0]).unwrap();
        println!("{:#?}", res);
    }
}
