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
    subquery_base_index: usize,
}

// FIXME: remove dead_code after we used parent context
#[derive(Default, Clone, Debug)]
#[allow(dead_code)]
struct BinderContext {
    /// table_name == table_id
    /// table_id -> table_catalog
    tables: HashMap<String, TableCatalog>,
    aliases: HashMap<String, BoundExpr>,
    parent: Option<Box<BinderContext>>,
}

impl Binder {
    pub fn new(catalog: RootCatalogRef) -> Self {
        Self {
            catalog,
            context: BinderContext::default(),
            subquery_base_index: 0,
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

impl BinderContext {
    pub fn new() -> Self {
        Self {
            parent: None,
            ..Default::default()
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
    #[error("subquery in FROM must have an alias")]
    SubqueryMustHaveAlias,
}

#[cfg(test)]
mod binder_test {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;
    use test_util::*;

    use super::*;
    use crate::catalog::RootCatalog;
    use crate::parser::parse;

    fn build_test_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        let table_id = "t1".to_string();
        let table_catalog = build_table_catalog(table_id.as_str(), vec!["c1", "c2"]);
        catalog.tables.insert(table_id, table_catalog);
        catalog
    }

    fn build_test_join_catalog() -> RootCatalog {
        let mut catalog = RootCatalog::new();
        catalog.tables.insert(
            "t1".to_string(),
            build_table_catalog("t1", vec!["c1", "c2"]),
        );
        catalog.tables.insert(
            "t2".to_string(),
            build_table_catalog("t2", vec!["c1", "c2"]),
        );
        catalog.tables.insert(
            "t3".to_string(),
            build_table_catalog("t3", vec!["c1", "c2"]),
        );
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
                if let BoundTableRef::Table(table) = select.from_table.unwrap() {
                    assert_eq!(table.catalog.id, "t1");
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
        let stats = parse(
            "select t1.c1, t2.c1, t3.c1 from t1 
                inner join t2 on t2.c1 = t1.c1 and t1.c2 = t2.c2
                left join t3 on t2.c1 = t3.c1 and t2.c2 > 1",
        )
        .unwrap();

        let bound_stmt = binder.bind(&stats[0]).unwrap();
        match bound_stmt {
            BoundStatement::Select(select) => {
                assert_eq!(select.select_list.len(), 3);
                assert!(select.from_table.is_some());
                let table = select.from_table.unwrap();
                assert_matches!(table, BoundTableRef::Join { .. });
                if let BoundTableRef::Join(join) = table {
                    assert_eq!(join.join_type, JoinType::Left);
                    assert_eq!(
                        join.join_condition,
                        JoinCondition::On {
                            on: vec![(
                                build_bound_column_ref("t2", "c1"),
                                build_bound_column_ref("t3", "c1"),
                            )],
                            filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                                op: BinaryOperator::Gt,
                                left: build_bound_column_ref_box("t2", "c2"),
                                right: build_int32_expr_box(1),
                                return_type: Some(DataType::Boolean),
                            })),
                        }
                    );
                    assert_eq!(*join.right, build_table_ref("t3", vec!["c1", "c2"]));
                    assert_eq!(
                        *join.left,
                        BoundTableRef::Join(Join {
                            left: build_table_ref_box("t1", vec!["c1", "c2"]),
                            right: build_table_ref_box("t2", vec!["c1", "c2"]),
                            join_type: JoinType::Inner,
                            join_condition: JoinCondition::On {
                                on: vec![
                                    (
                                        build_bound_column_ref("t1", "c1"),
                                        build_bound_column_ref("t2", "c1")
                                    ),
                                    (
                                        build_bound_column_ref("t1", "c2"),
                                        build_bound_column_ref("t2", "c2"),
                                    )
                                ],
                                filter: None,
                            }
                        })
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
                        expr: build_bound_column_ref("t1", "c2"),
                        asc: false,
                    }
                );
                assert_eq!(
                    select.order_by[1],
                    BoundOrderBy {
                        expr: build_bound_column_ref("t1", "c1"),
                        asc: true,
                    }
                );
            }
        }
    }
}

pub mod test_util {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType;

    use super::*;
    use crate::catalog::*;
    use crate::types::ScalarValue;

    pub fn build_bound_constant(val: i32) -> BoundExpr {
        BoundExpr::Constant(val.into())
    }

    pub fn build_bound_column_ref_box(table_id: &str, name: &str) -> Box<BoundExpr> {
        Box::new(BoundExpr::ColumnRef(build_bound_column_ref_internal(
            table_id, name,
        )))
    }

    pub fn build_bound_column_ref(table_id: &str, name: &str) -> BoundExpr {
        BoundExpr::ColumnRef(build_bound_column_ref_internal(table_id, name))
    }

    pub fn build_bound_column_ref_internal(table_id: &str, name: &str) -> BoundColumnRef {
        BoundColumnRef {
            column_catalog: build_column_catalog(table_id, name),
        }
    }

    pub fn build_column_catalog(table_id: &str, name: &str) -> ColumnCatalog {
        ColumnCatalog {
            table_id: table_id.to_string(),
            column_id: name.to_string(),
            desc: ColumnDesc {
                name: name.to_string(),
                data_type: DataType::Int32,
            },
            nullable: true,
        }
    }

    pub fn build_columns_catalog(
        table_id: &str,
        columns: Vec<&str>,
        nullable: bool,
    ) -> Vec<ColumnCatalog> {
        columns
            .iter()
            .map(|c| ColumnCatalog {
                table_id: table_id.to_string(),
                column_id: c.to_string(),
                desc: ColumnDesc {
                    name: c.to_string(),
                    data_type: DataType::Int32,
                },
                nullable,
            })
            .collect::<_>()
    }

    pub fn build_table_catalog(table_id: &str, columns: Vec<&str>) -> TableCatalog {
        let mut columns_tree = BTreeMap::new();
        for c in &columns {
            columns_tree.insert(c.to_string(), build_column_catalog(table_id, c));
        }
        let column_ids = columns.iter().map(|c| c.to_string()).collect();
        TableCatalog {
            id: table_id.to_string(),
            name: table_id.to_string(),
            columns: columns_tree,
            column_ids,
        }
    }

    pub fn build_table_ref(table_id: &str, columns: Vec<&str>) -> BoundTableRef {
        BoundTableRef::Table(BoundSimpleTable::new(
            build_table_catalog(table_id, columns),
            None,
        ))
    }

    pub fn build_table_ref_box(table_id: &str, columns: Vec<&str>) -> Box<BoundTableRef> {
        Box::new(build_table_ref(table_id, columns))
    }

    pub fn build_int32_expr_box(v: i32) -> Box<BoundExpr> {
        Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(v))))
    }

    pub fn build_join_condition_eq(
        left_join_table: &str,
        left_join_column: &str,
        right_join_table: &str,
        right_join_column: &str,
    ) -> JoinCondition {
        JoinCondition::On {
            on: vec![(
                build_bound_column_ref(left_join_table, left_join_column),
                build_bound_column_ref(right_join_table, right_join_column),
            )],
            filter: None,
        }
    }

    pub fn build_bound_input_ref_box(index: usize) -> Box<BoundExpr> {
        Box::new(BoundExpr::InputRef(BoundInputRef {
            index,
            return_type: DataType::Int32,
        }))
    }

    pub fn build_bound_input_ref(index: usize) -> BoundExpr {
        BoundExpr::InputRef(BoundInputRef {
            index,
            return_type: DataType::Int32,
        })
    }
}
