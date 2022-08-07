mod select;
mod util;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRef;

pub struct Planner {}

impl Planner {
    pub fn plan(&self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        match stmt {
            BoundStatement::Select(stmt) => self.plan_select(stmt),
        }
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum LogicalPlanError {}

#[cfg(test)]
mod planner_test {
    use std::collections::BTreeMap;

    use arrow::datatypes::DataType::{self, Int32};
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::{
        BoundBinaryOp, BoundColumnRef, BoundExpr, BoundSelect, BoundStatement, BoundTableRef, Join,
        JoinCondition, JoinType,
    };
    use crate::catalog::{ColumnCatalog, ColumnDesc, TableCatalog};
    use crate::optimizer::PlanNodeType;
    use crate::types::ScalarValue;

    fn build_test_column(table_id: String, column_name: String) -> BoundExpr {
        BoundExpr::ColumnRef(BoundColumnRef {
            column_catalog: ColumnCatalog {
                table_id,
                column_id: column_name.clone(),
                desc: ColumnDesc {
                    name: column_name,
                    data_type: Int32,
                },
            },
        })
    }

    fn build_test_table(table_name: String, columns: Vec<String>) -> Option<BoundTableRef> {
        let mut column_map = BTreeMap::new();
        let mut column_ids = Vec::new();
        for column in columns {
            column_ids.push(column.clone());
            column_map.insert(
                column.clone(),
                ColumnCatalog {
                    table_id: table_name.clone(),
                    column_id: column.clone(),
                    desc: ColumnDesc {
                        name: column,
                        data_type: Int32,
                    },
                },
            );
        }
        Some(BoundTableRef::Table(TableCatalog {
            id: table_name.clone(),
            name: table_name,
            columns: column_map,
            column_ids,
        }))
    }

    fn build_test_select_stmt() -> BoundStatement {
        let table_id = "t".to_string();
        let c1 = build_test_column(table_id.clone(), "c1".to_string());
        let t = build_test_table(table_id.clone(), vec!["c1".to_string(), "c2".to_string()]);

        let where_clause = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Eq,
            left: Box::new(build_test_column(table_id, "c2".to_string())),
            right: Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(2)))),
            return_type: Some(DataType::Boolean),
        });

        BoundStatement::Select(BoundSelect {
            select_list: vec![c1],
            from_table: t,
            where_clause: Some(where_clause),
            group_by: vec![],
            limit: Some(BoundExpr::Constant(10.into())),
            offset: None,
            order_by: vec![],
        })
    }

    fn build_join_condition_eq(
        left_join_table: String,
        left_join_column: String,
        right_join_table: String,
        right_join_column: String,
    ) -> JoinCondition {
        JoinCondition::On(BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Eq,
            left: Box::new(build_test_column(left_join_table, left_join_column)),
            right: Box::new(build_test_column(right_join_table, right_join_column)),
            return_type: Some(DataType::Boolean),
        }))
    }

    fn build_test_select_stmt_with_multiple_joins() -> BoundStatement {
        let t1 = "t1".to_string();
        let t2 = "t2".to_string();
        let t3 = "t3".to_string();
        let t1_ref =
            build_test_table(t1.clone(), vec!["c1".to_string(), "c2".to_string()]).unwrap();
        let t2_ref =
            build_test_table(t2.clone(), vec!["c1".to_string(), "c2".to_string()]).unwrap();
        let t3_ref = build_test_table(t3, vec!["c1".to_string(), "c2".to_string()]).unwrap();
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1=t2.c1
        // left join t3 on t2.c1=t3.c1
        let table_ref = BoundTableRef::Join(Join {
            left: Box::new(BoundTableRef::Join(Join {
                left: Box::new(t1_ref),
                right: Box::new(t2_ref),
                join_type: JoinType::Inner,
                join_condition: build_join_condition_eq(
                    "t1".to_string(),
                    "c1".to_string(),
                    "t2".to_string(),
                    "c1".to_string(),
                ),
            })),
            right: Box::new(t3_ref),
            join_type: JoinType::Left,
            join_condition: build_join_condition_eq(
                "t2".to_string(),
                "c1".to_string(),
                "t3".to_string(),
                "c1".to_string(),
            ),
        });

        BoundStatement::Select(BoundSelect {
            select_list: vec![
                build_test_column(t1, "c1".to_string()),
                build_test_column(t2, "c1".to_string()),
            ],
            from_table: Some(table_ref),
            where_clause: None,
            group_by: vec![],
            limit: None,
            offset: None,
            order_by: vec![],
        })
    }

    #[test]
    fn test_plan_select_works() {
        let stmt = build_test_select_stmt();
        let p = Planner {};
        let node = p.plan(stmt);
        assert!(node.is_ok());
        let plan_ref = node.unwrap();
        assert_eq!(plan_ref.node_type(), PlanNodeType::LogicalLimit);
        assert_eq!(plan_ref.schema().len(), 2);
        dbg!(plan_ref);
    }

    #[test]
    fn test_plan_select_with_joins_works() {
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1=t2.c1
        // left join t3 on t2.c1=t3.c1
        let stmt = build_test_select_stmt_with_multiple_joins();
        let p = Planner {};
        let node = p.plan(stmt);
        assert!(node.is_ok());
        let plan_ref = node.unwrap();
        assert_eq!(plan_ref.node_type(), PlanNodeType::LogicalProject);
        let plan_node = &plan_ref.children()[0];
        let join_plan = plan_node.as_logical_join().unwrap();

        // check join right part: left join t3 on t2.c1=t3.c1
        let right = join_plan.right();
        assert_eq!(
            right.as_logical_table_scan().unwrap().table_id(),
            "t3".to_string()
        );
        assert_eq!(join_plan.join_type(), JoinType::Left);
        assert_eq!(
            join_plan.join_condition(),
            build_join_condition_eq(
                "t2".to_string(),
                "c1".to_string(),
                "t3".to_string(),
                "c1".to_string()
            )
        );

        // check join left part: inner join t2 on t1.c1=t2.c1
        let left = join_plan.left();
        let left_as_join = left.as_logical_join().unwrap();
        assert_eq!(left_as_join.join_type(), JoinType::Inner);
        assert_eq!(
            left_as_join
                .left()
                .as_logical_table_scan()
                .unwrap()
                .table_id(),
            "t1".to_string()
        );
        assert_eq!(
            left_as_join
                .right()
                .as_logical_table_scan()
                .unwrap()
                .table_id(),
            "t2".to_string()
        );
        assert_eq!(
            left_as_join.join_condition(),
            build_join_condition_eq(
                "t1".to_string(),
                "c1".to_string(),
                "t2".to_string(),
                "c1".to_string()
            )
        );

        dbg!(plan_ref);
    }
}
