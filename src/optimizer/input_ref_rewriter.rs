use std::sync::Arc;

use super::expr_rewriter::ExprRewriter;
use super::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOrder, LogicalProject,
    LogicalTableScan, PlanRef, PlanRewriter,
};
use crate::binder::{BoundColumnRef, BoundExpr, BoundInputRef};

#[derive(Default)]
pub struct InputRefRewriter {
    /// The bound exprs of the last visited plan node, which is used to resolve the index of
    /// RecordBatch.
    bindings: Vec<BoundExpr>,
}

impl InputRefRewriter {
    fn rewrite_internal(&self, expr: &mut BoundExpr) {
        // Find input expr in bindings.
        if let Some(idx) = self.bindings.iter().position(|e| *e == expr.clone()) {
            *expr = BoundExpr::InputRef(BoundInputRef {
                index: idx,
                return_type: expr.return_type().unwrap(),
            });
            return;
        }

        // If not found in bindings, expand nested expr and then continuity rewrite_expr.
        match expr {
            BoundExpr::BinaryOp(e) => {
                self.rewrite_expr(e.left.as_mut());
                self.rewrite_expr(e.right.as_mut());
            }
            BoundExpr::TypeCast(e) => self.rewrite_expr(e.expr.as_mut()),
            BoundExpr::AggFunc(e) => {
                for arg in &mut e.exprs {
                    self.rewrite_expr(arg);
                }
            }
            _ => unreachable!(
                "unexpected expr type {:?} for InputRefRewriter, binding: {:?}",
                expr, self.bindings
            ),
        }
    }
}

impl ExprRewriter for InputRefRewriter {
    fn rewrite_column_ref(&self, expr: &mut BoundExpr) {
        self.rewrite_internal(expr);
    }

    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        self.rewrite_internal(expr);
    }

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        self.rewrite_internal(expr);
    }

    fn rewrite_agg_func(&self, expr: &mut BoundExpr) {
        self.rewrite_internal(expr);
    }
}

impl PlanRewriter for InputRefRewriter {
    fn rewrite_logical_table_scan(&mut self, plan: &LogicalTableScan) -> PlanRef {
        self.bindings = plan
            .columns()
            .iter()
            .map(|c| {
                BoundExpr::ColumnRef(BoundColumnRef {
                    column_catalog: c.clone(),
                })
            })
            .collect();
        Arc::new(plan.clone())
    }

    fn rewrite_logical_join(&mut self, plan: &LogicalJoin) -> PlanRef {
        let new_left = self.rewrite(plan.left());
        let mut right_input_ref_rewriter = InputRefRewriter::default();
        let new_right = right_input_ref_rewriter.rewrite(plan.right());

        // combine the bindings of left and right, and consumed by upper logical plan, such as
        // LogicalProject.
        self.bindings.append(&mut right_input_ref_rewriter.bindings);
        Arc::new(LogicalJoin::new(
            new_left,
            new_right,
            plan.join_type(),
            plan.join_condition(),
        ))
    }

    fn rewrite_logical_project(&mut self, plan: &LogicalProject) -> PlanRef {
        let new_child = self.rewrite(plan.input());

        let bindings = plan.exprs();

        let mut new_exprs = plan.exprs();
        for expr in &mut new_exprs {
            self.rewrite_expr(expr);
        }

        self.bindings = bindings;
        let new_plan = LogicalProject::new(new_exprs, new_child);
        Arc::new(new_plan)
    }

    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let new_child = self.rewrite(plan.input());

        let mut new_expr = plan.expr();
        self.rewrite_expr(&mut new_expr);

        let new_plan = LogicalFilter::new(new_expr, new_child);
        Arc::new(new_plan)
    }

    fn rewrite_logical_limit(&mut self, plan: &LogicalLimit) -> PlanRef {
        let new_child = self.rewrite(plan.input());
        let new_limit = match plan.limit() {
            Some(mut limit) => {
                self.rewrite_expr(&mut limit);
                Some(limit)
            }
            None => None,
        };
        let new_offset = match plan.offset() {
            Some(mut offset) => {
                self.rewrite_expr(&mut offset);
                Some(offset)
            }
            None => None,
        };
        let new_plan = LogicalLimit::new(new_limit, new_offset, new_child);
        Arc::new(new_plan)
    }

    fn rewrite_logical_order(&mut self, plan: &LogicalOrder) -> PlanRef {
        let new_child = self.rewrite(plan.input());
        let mut new_order_by = plan.order_by();
        for expr in &mut new_order_by {
            self.rewrite_expr(&mut expr.expr);
        }
        let new_plan = LogicalOrder::new(new_order_by, new_child);
        Arc::new(new_plan)
    }

    fn rewrite_logical_agg(&mut self, plan: &LogicalAgg) -> PlanRef {
        let new_child = self.rewrite(plan.input());
        let bindings = plan
            .group_by()
            .iter()
            .chain(plan.agg_funcs().iter())
            .cloned()
            .collect();

        let mut new_agg_funcs = plan.agg_funcs();
        for expr in &mut new_agg_funcs {
            self.rewrite_expr(expr);
        }

        let mut new_group_exprs = plan.group_by();
        for expr in &mut new_group_exprs {
            self.rewrite_expr(expr);
        }

        self.bindings = bindings;
        let new_plan = LogicalAgg::new(new_agg_funcs, new_group_exprs, new_child);
        Arc::new(new_plan)
    }
}

#[cfg(test)]
mod input_ref_rewriter_test {
    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::{
        AggFunc, BoundAggFunc, BoundBinaryOp, BoundOrderBy, JoinCondition, JoinType,
    };
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::optimizer::LogicalOrder;
    use crate::types::ScalarValue;

    fn build_bound_column_ref_internal(table_id: String, name: String) -> BoundColumnRef {
        BoundColumnRef {
            column_catalog: build_test_column_catalog(table_id, name),
        }
    }

    fn build_test_column(table_id: String, column_name: String) -> BoundExpr {
        BoundExpr::ColumnRef(BoundColumnRef {
            column_catalog: build_test_column_catalog(table_id, column_name),
        })
    }

    fn build_test_column_catalog(table_id: String, column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            table_id,
            column_id: column_name.clone(),
            desc: ColumnDesc {
                name: column_name,
                data_type: DataType::Int32,
            },
        }
    }

    fn build_join_condition_eq(
        left_join_table: String,
        left_join_column: String,
        right_join_table: String,
        right_join_column: String,
    ) -> JoinCondition {
        JoinCondition::On {
            on: vec![(
                build_bound_column_ref_internal(left_join_table, left_join_column),
                build_bound_column_ref_internal(right_join_table, right_join_column),
            )],
            filter: None,
        }
    }

    fn build_logical_table_scan(table_id: String) -> LogicalTableScan {
        LogicalTableScan::new(
            table_id.clone(),
            vec![
                build_test_column_catalog(table_id.clone(), "c1".to_string()),
                build_test_column_catalog(table_id, "c2".to_string()),
            ],
        )
    }

    fn build_logical_project(input: PlanRef) -> LogicalProject {
        LogicalProject::new(
            vec![BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: build_test_column_catalog("t".to_string(), "c2".to_string()),
            })],
            input,
        )
    }

    fn build_logical_filter(input: PlanRef) -> LogicalFilter {
        LogicalFilter::new(
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Eq,
                left: Box::new(BoundExpr::ColumnRef(BoundColumnRef {
                    column_catalog: build_test_column_catalog("t".to_string(), "c1".to_string()),
                })),
                right: Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(2)))),
                return_type: Some(DataType::Boolean),
            }),
            input,
        )
    }

    fn build_logical_project_with_simple_agg(input: PlanRef) -> LogicalProject {
        let expr = BoundExpr::AggFunc(BoundAggFunc {
            func: AggFunc::Sum,
            exprs: vec![BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: build_test_column_catalog("t".to_string(), "c1".to_string()),
            })],
            return_type: DataType::Int32,
        });
        let simple_agg = LogicalAgg::new(vec![expr.clone()], vec![], input);
        LogicalProject::new(vec![expr], Arc::new(simple_agg))
    }

    fn build_logical_limit(input: PlanRef) -> LogicalLimit {
        LogicalLimit::new(Some(BoundExpr::Constant(10.into())), None, input)
    }

    fn build_logical_order(input: PlanRef) -> LogicalOrder {
        let order_by = vec![BoundOrderBy {
            expr: BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: build_test_column_catalog("t".to_string(), "c1".to_string()),
            }),
            asc: false,
        }];
        LogicalOrder::new(order_by, input)
    }

    fn build_logical_joins() -> LogicalJoin {
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1=t2.c1
        // left join t3 on t2.c1=t3.c1
        LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                Arc::new(build_logical_table_scan("t1".to_string())),
                Arc::new(build_logical_table_scan("t2".to_string())),
                JoinType::Inner,
                build_join_condition_eq(
                    "t1".to_string(),
                    "c1".to_string(),
                    "t2".to_string(),
                    "c1".to_string(),
                ),
            )),
            Arc::new(build_logical_table_scan("t3".to_string())),
            JoinType::Left,
            build_join_condition_eq(
                "t2".to_string(),
                "c1".to_string(),
                "t3".to_string(),
                "c1".to_string(),
            ),
        )
    }

    #[test]
    fn test_rewrite_column_ref_to_input_ref() {
        let plan = build_logical_table_scan("t".to_string());
        let filter_plan = build_logical_filter(Arc::new(plan));
        let project_plan = build_logical_project(Arc::new(filter_plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(project_plan));

        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs(),
            vec![BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: DataType::Int32,
            })]
        );
        assert_eq!(
            new_plan.children()[0].as_logical_filter().unwrap().expr(),
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Eq,
                left: Box::new(BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataType::Int32,
                })),
                right: Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(2)))),
                return_type: Some(DataType::Boolean),
            })
        );
    }

    #[test]
    fn test_rewrite_column_ref_to_input_ref_when_join() {
        let plan = build_logical_joins();
        let project_plan = LogicalProject::new(
            vec![
                build_test_column("t1".to_string(), "c1".to_string()),
                build_test_column("t2".to_string(), "c1".to_string()),
                build_test_column("t3".to_string(), "c1".to_string()),
            ],
            Arc::new(plan),
        );

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(project_plan));

        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs(),
            vec![
                BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataType::Int32,
                }),
                BoundExpr::InputRef(BoundInputRef {
                    index: 2,
                    return_type: DataType::Int32,
                }),
                BoundExpr::InputRef(BoundInputRef {
                    index: 4,
                    return_type: DataType::Int32,
                })
            ]
        );
    }

    #[test]
    fn test_rewrite_simple_aggregation_column_ref_to_input_ref() {
        let plan = build_logical_table_scan("t".to_string());
        let plan = build_logical_project_with_simple_agg(Arc::new(plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(plan));

        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs(),
            vec![BoundExpr::InputRef(BoundInputRef {
                index: 0,
                return_type: DataType::Int32,
            })]
        );
    }

    #[test]
    fn test_rewrite_limit_to_input_ref() {
        let plan = build_logical_table_scan("t".to_string());
        let plan = build_logical_limit(Arc::new(plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(plan));

        assert_eq!(
            new_plan.as_logical_limit().unwrap().limit(),
            Some(BoundExpr::Constant(ScalarValue::Int32(Some(10))))
        );
        assert_eq!(new_plan.as_logical_limit().unwrap().offset(), None);
    }

    #[test]
    fn test_rewrite_order_by_to_input_ref() {
        let plan = build_logical_table_scan("t".to_string());
        let plan = build_logical_order(Arc::new(plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(plan));

        assert_eq!(
            new_plan.as_logical_order().unwrap().order_by()[0],
            BoundOrderBy {
                expr: BoundExpr::InputRef(BoundInputRef {
                    index: 0,
                    return_type: DataType::Int32,
                }),
                asc: false,
            }
        );
    }
}
