mod expression;
mod statement;
mod table;

use std::collections::HashMap;

pub use expression::*;
use sqlparser::ast::{Ident, Statement};
pub use statement::*;
pub use table::*;

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
    #[error("ambiguous column {0}")]
    AmbiguousColumn(String),
    #[error("binary operator types mismatch: {0} != {1}")]
    BinaryOpTypeMismatch(String, String),
}

#[cfg(test)]
mod binder_test {
    use std::assert_matches::assert_matches;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::catalog::{ColumnCatalog, ColumnDesc, RootCatalog};
    use crate::parser::parse;

    fn build_column_catalog(table_id: String, name: String) -> ColumnCatalog {
        ColumnCatalog {
            table_id,
            column_id: name.clone(),
            desc: ColumnDesc {
                name,
                data_type: DataType::Int32,
            },
        }
    }

    fn build_table_catalog(table_id: String) -> TableCatalog {
        let mut columns = BTreeMap::new();
        columns.insert(
            "c1".to_string(),
            build_column_catalog(table_id.clone(), "c1".to_string()),
        );
        columns.insert(
            "c2".to_string(),
            build_column_catalog(table_id.clone(), "c2".to_string()),
        );
        let column_ids = vec!["c1".to_string(), "c2".to_string()];
        TableCatalog {
            id: table_id.clone(),
            name: table_id,
            columns,
            column_ids,
        }
    }

    fn build_test_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        let table_id = "t1".to_string();
        let table_catalog = build_table_catalog(table_id.clone());
        catalog.tables.insert(table_id, table_catalog);
        catalog
    }

    fn build_test_join_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        let t1 = "t1".to_string();
        let t2 = "t2".to_string();
        catalog.tables.insert(t1.clone(), build_table_catalog(t1));
        catalog.tables.insert(t2.clone(), build_table_catalog(t2));
        catalog
    }

    #[test]
    fn test_bind_select_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1, c2 from t1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 2);
                assert!(select.from_table.is_some());
                if let BoundTableRef::Table { table_catalog } = select.from_table.unwrap() {
                    assert_eq!(table_catalog.id, "t1");
                }
            }
        }
    }

    #[test]
    fn test_check_ambiguous_columns_works() {
        let catalog = build_test_join_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1, c1 from t1 inner join t2 on t1.c1 = t2.c1").unwrap();
        let stmt = binder.bind(&stats[0]);
        assert_matches!(stmt, Err(BindError::AmbiguousColumn(_)));
        match stmt {
            Ok(_) => unreachable!(),
            Err(err) => assert_eq!(err.to_string(), "ambiguous column c1"),
        }
    }

    #[test]
    fn test_bind_join_works() {
        let catalog = build_test_join_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select t1.c1, t2.c2 from t1 inner join t2 on t1.c1 = t2.c1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 2);
                assert!(select.from_table.is_some());
                let table = select.from_table.unwrap();
                assert_matches!(table, BoundTableRef::Join { .. });
                if let BoundTableRef::Join { relation: _, joins } = table {
                    assert_eq!(joins[0].join_type, JoinType::Inner);
                    assert_eq!(
                        joins[0].join_condition,
                        JoinCondition::On(BoundExpr::BinaryOp(BoundBinaryOp {
                            op: BinaryOperator::Eq,
                            left: Box::new(BoundExpr::ColumnRef(BoundColumnRef {
                                column_catalog: build_column_catalog(
                                    "t1".to_string(),
                                    "c1".to_string()
                                )
                            })),
                            right: Box::new(BoundExpr::ColumnRef(BoundColumnRef {
                                column_catalog: build_column_catalog(
                                    "t2".to_string(),
                                    "c1".to_string()
                                )
                            })),
                            return_type: Some(DataType::Boolean)
                        }))
                    );
                }
            }
        }
    }

    #[test]
    fn test_bind_select_constant_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select 1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 1);
                assert!(select.from_table.is_none());
            }
        }
    }

    #[test]
    fn test_bind_select_binary_op_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1 from t1 where c2 = 1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert!(select.where_clause.is_some());
            }
        }
    }

    #[test]
    fn test_bind_select_agg_func_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select sum(c1), count(c2) from t1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_matches!(select.select_list[0], BoundExpr::AggFunc(_));
                assert_matches!(select.select_list[1], BoundExpr::AggFunc(_));
            }
        }
    }

    #[test]
    fn test_bind_select_limit_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1 from t1 limit 1 offset 10").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.limit, Some(BoundExpr::Constant(1.into())));
                assert_eq!(select.offset, Some(BoundExpr::Constant(10.into())));
            }
        }
    }

    #[test]
    fn test_bind_select_order_by_works() {
        let catalog = build_test_catalog();
        let mut binder = Binder::new(Arc::new(catalog));
        let stats = parse("select c1 from t1 order by c2 desc, c1").unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(
                    select.order_by[0],
                    BoundOrderBy {
                        expr: BoundExpr::ColumnRef(BoundColumnRef {
                            column_catalog: build_column_catalog(
                                "t1".to_string(),
                                "c2".to_string()
                            )
                        }),
                        asc: false,
                    }
                );
                assert_eq!(
                    select.order_by[1],
                    BoundOrderBy {
                        expr: BoundExpr::ColumnRef(BoundColumnRef {
                            column_catalog: build_column_catalog(
                                "t1".to_string(),
                                "c1".to_string()
                            )
                        }),
                        asc: true,
                    }
                );
            }
        }
    }
}
