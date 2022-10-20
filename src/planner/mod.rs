mod select;
mod util;

use std::collections::HashMap;

use crate::binder::BoundStatement;
use crate::optimizer::PlanRef;

#[derive(Default)]
pub struct Planner {
    pub context: PlannerContext,
}

#[derive(Default, Debug)]
pub struct PlannerContext {
    // subquery alias to subquery plan
    pub subquery_context: HashMap<String, PlanRef>,
}

impl PlannerContext {
    pub fn find_subquery_alias(&self, plan_ref: &PlanRef) -> Option<String> {
        for (alias, p) in &self.subquery_context {
            if p == plan_ref {
                return Some(alias.clone());
            }
        }
        None
    }
}

impl Planner {
    pub fn plan(&mut self, stmt: BoundStatement) -> Result<PlanRef, LogicalPlanError> {
        match stmt {
            BoundStatement::Select(stmt) => self.plan_select(stmt),
        }
    }
}

#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum LogicalPlanError {}

#[cfg(test)]
mod planner_test {

    use arrow::datatypes::DataType::{self};
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::{
        BoundBinaryOp, BoundExpr, BoundSelect, BoundStatement, BoundTableRef, Join, JoinType,
    };
    use crate::optimizer::PlanNodeType;

    fn build_test_select_distinct_stmt() -> BoundStatement {
        let c1 = build_bound_column_ref("t", "c1");
        let t = build_table_ref("t", vec!["c1", "c2"]);

        BoundStatement::Select(BoundSelect {
            select_list: vec![c1],
            from_table: Some(t),
            where_clause: None,
            group_by: vec![],
            limit: None,
            offset: None,
            order_by: vec![],
            select_distinct: true,
        })
    }

    fn build_test_select_stmt() -> BoundStatement {
        let c1 = build_bound_column_ref("t", "c1");
        let t = build_table_ref("t", vec!["c1", "c2"]);

        let where_clause = BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::Eq,
            left: build_bound_column_ref_box("t", "c2"),
            right: build_int32_expr_box(2),
            return_type: Some(DataType::Boolean),
        });

        BoundStatement::Select(BoundSelect {
            select_list: vec![c1],
            from_table: Some(t),
            where_clause: Some(where_clause),
            group_by: vec![],
            limit: Some(BoundExpr::Constant(10.into())),
            offset: None,
            order_by: vec![],
            select_distinct: false,
        })
    }

    fn build_test_select_stmt_with_multiple_joins() -> BoundStatement {
        let t1_ref = build_table_ref_box("t1", vec!["c1", "c2"]);
        let t2_ref = build_table_ref_box("t2", vec!["c1", "c2"]);
        let t3_ref = build_table_ref_box("t3", vec!["c1", "c2"]);
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1=t2.c1
        // left join t3 on t2.c1=t3.c1
        let table_ref = BoundTableRef::Join(Join {
            left: Box::new(BoundTableRef::Join(Join {
                left: t1_ref,
                right: t2_ref,
                join_type: JoinType::Inner,
                join_condition: build_join_condition_eq("t1", "c1", "t2", "c1"),
            })),
            right: t3_ref,
            join_type: JoinType::Left,
            join_condition: build_join_condition_eq("t2", "c1", "t3", "c1"),
        });

        BoundStatement::Select(BoundSelect {
            select_list: vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
            ],
            from_table: Some(table_ref),
            where_clause: None,
            group_by: vec![],
            limit: None,
            offset: None,
            order_by: vec![],
            select_distinct: false,
        })
    }

    #[test]
    fn test_plan_select_works() {
        let stmt = build_test_select_stmt();
        let mut p = Planner::default();
        let node = p.plan(stmt);
        assert!(node.is_ok());
        let plan_ref = node.unwrap();
        assert_eq!(plan_ref.node_type(), PlanNodeType::LogicalLimit);
        assert_eq!(plan_ref.output_columns().len(), 1);
        dbg!(plan_ref);
    }

    #[test]
    fn test_plan_select_with_joins_works() {
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1=t2.c1
        // left join t3 on t2.c1=t3.c1
        let stmt = build_test_select_stmt_with_multiple_joins();
        let mut p = Planner::default();
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
            build_join_condition_eq("t2", "c1", "t3", "c1")
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
            build_join_condition_eq("t1", "c1", "t2", "c1")
        );

        dbg!(plan_ref);
    }

    #[test]
    fn test_plan_select_distinct_works() {
        let stmt = build_test_select_distinct_stmt();
        let mut p = Planner::default();
        let node = p.plan(stmt);
        assert!(node.is_ok());
        let plan_ref = node.unwrap();
        assert_eq!(plan_ref.node_type(), PlanNodeType::LogicalProject);
        assert_eq!(plan_ref.children()[0].node_type(), PlanNodeType::LogicalAgg);
        dbg!(plan_ref);
    }
}
