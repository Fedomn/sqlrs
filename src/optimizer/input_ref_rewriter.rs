use std::sync::Arc;

use super::expr_rewriter::ExprRewriter;
use super::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOrder, LogicalProject,
    LogicalTableScan, PhysicalFilter, PhysicalHashAgg, PhysicalHashJoin, PhysicalLimit,
    PhysicalOrder, PhysicalProject, PhysicalSimpleAgg, PhysicalTableScan, PlanRef, PlanRewriter,
};
use crate::binder::{BoundColumnRef, BoundExpr, BoundInputRef, JoinCondition};

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
    fn rewrite_physical_table_scan(&mut self, plan: &PhysicalTableScan) -> PlanRef {
        let logical = self.rewrite_logical_table_scan(plan.logical());
        Arc::new(PhysicalTableScan::new(
            logical.as_logical_table_scan().unwrap().clone(),
        ))
    }

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

    /// In join executor internal logic, the join condition `on(expr1, expr2)` is
    /// spereated into left and right keys, and them are against on different RecordBatch,
    /// so the left and right InputRef index should be resolved using different bindings.
    /// For join condition `filter` expr, it will against on the last RecordBatch which is merged
    /// from left and right RecordBatch, so its InputRef index be resolved using merged bindings.
    fn rewrite_logical_join(&mut self, plan: &LogicalJoin) -> PlanRef {
        let original_join_output_columns = plan.join_output_columns();

        let new_left = self.rewrite(plan.left());
        let mut right_input_ref_rewriter = InputRefRewriter::default();
        let new_right = right_input_ref_rewriter.rewrite(plan.right());

        // rewrite the condition expr. left index and right index are calculated from different
        // binding.
        let new_on = if let JoinCondition::On { on, filter: _ } = plan.join_condition() {
            let mut on_left_keys = on.iter().map(|o| o.0.clone()).collect::<Vec<_>>();
            let mut on_right_keys = on.iter().map(|o| o.1.clone()).collect::<Vec<_>>();

            // 1.use left bindings to rewrite left keys.
            for expr in &mut on_left_keys {
                self.rewrite_expr(expr);
            }

            // 2.use right bindings to rewrite right keys.
            for expr in &mut on_right_keys {
                right_input_ref_rewriter.rewrite_expr(expr);
            }

            // 3.combine left and right keys into new tuples.
            let new_on = on_left_keys
                .into_iter()
                .zip(on_right_keys.into_iter())
                .map(|(l, r)| (l, r))
                .collect::<Vec<_>>();
            Some(new_on)
        } else {
            None
        };

        // 4.combine the bindings of left and right, and consumed by upper logical plan, such as
        // LogicalProject.
        self.bindings.append(&mut right_input_ref_rewriter.bindings);

        // 5.use merged bindings(left + right) to rewrite condition and the filter will against on
        // result batch in join executor.
        let new_join_condition = if let JoinCondition::On { on: _, filter } = plan.join_condition()
        {
            let new_filter = match filter {
                Some(mut expr) => {
                    self.rewrite_expr(&mut expr);
                    Some(expr)
                }
                None => None,
            };
            JoinCondition::On {
                on: new_on.unwrap(),
                filter: new_filter,
            }
        } else {
            plan.join_condition()
        };

        Arc::new(LogicalJoin::new_with_output_columns(
            new_left,
            new_right,
            plan.join_type(),
            new_join_condition,
            original_join_output_columns,
        ))
    }

    fn rewrite_physical_hash_join(&mut self, plan: &PhysicalHashJoin) -> PlanRef {
        let logical = self.rewrite_logical_join(plan.logical());
        Arc::new(PhysicalHashJoin::new(
            logical.as_logical_join().unwrap().clone(),
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

    fn rewrite_physical_project(&mut self, plan: &PhysicalProject) -> PlanRef {
        let logical = self.rewrite_logical_project(plan.logical());
        Arc::new(PhysicalProject::new(
            logical.as_logical_project().unwrap().clone(),
        ))
    }

    fn rewrite_logical_filter(&mut self, plan: &LogicalFilter) -> PlanRef {
        let new_child = self.rewrite(plan.input());

        let mut new_expr = plan.expr();
        self.rewrite_expr(&mut new_expr);

        let new_plan = LogicalFilter::new(new_expr, new_child);
        Arc::new(new_plan)
    }

    fn rewrite_physical_filter(&mut self, plan: &PhysicalFilter) -> PlanRef {
        let logical = self.rewrite_logical_filter(plan.logical());
        Arc::new(PhysicalFilter::new(
            logical.as_logical_filter().unwrap().clone(),
        ))
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

    fn rewrite_physical_limit(&mut self, plan: &PhysicalLimit) -> PlanRef {
        let logical = self.rewrite_logical_limit(plan.logical());
        Arc::new(PhysicalLimit::new(
            logical.as_logical_limit().unwrap().clone(),
        ))
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

    fn rewrite_physical_order(&mut self, plan: &PhysicalOrder) -> PlanRef {
        let logical = self.rewrite_logical_order(plan.logical());
        Arc::new(PhysicalOrder::new(
            logical.as_logical_order().unwrap().clone(),
        ))
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

    fn rewrite_physical_hash_agg(&mut self, plan: &PhysicalHashAgg) -> PlanRef {
        let logical = self.rewrite_logical_agg(plan.logical());
        Arc::new(PhysicalHashAgg::new(
            logical.as_logical_agg().unwrap().clone(),
        ))
    }

    fn rewrite_physical_simple_agg(&mut self, plan: &PhysicalSimpleAgg) -> PlanRef {
        let logical = self.rewrite_logical_agg(plan.logical());
        Arc::new(PhysicalSimpleAgg::new(
            logical.as_logical_agg().unwrap().clone(),
        ))
    }
}

#[cfg(test)]
mod input_ref_rewriter_test {
    use std::assert_matches::assert_matches;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::{AggFunc, BoundAggFunc, BoundBinaryOp, BoundOrderBy, JoinType};
    use crate::optimizer::LogicalOrder;
    use crate::types::ScalarValue;

    fn build_logical_table_scan(table_id: &str) -> LogicalTableScan {
        LogicalTableScan::new(
            table_id.to_string(),
            vec![
                build_column_catalog(table_id, "c1"),
                build_column_catalog(table_id, "c2"),
            ],
            None,
            None,
        )
    }

    fn build_logical_project(input: PlanRef) -> LogicalProject {
        LogicalProject::new(vec![build_bound_column_ref("t", "c2")], input)
    }

    fn build_logical_filter(input: PlanRef) -> LogicalFilter {
        LogicalFilter::new(
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Eq,
                left: build_bound_column_ref_box("t", "c1"),
                right: build_int32_expr_box(2),
                return_type: Some(DataType::Boolean),
            }),
            input,
        )
    }

    fn build_logical_project_with_simple_agg(input: PlanRef) -> LogicalProject {
        let expr = BoundExpr::AggFunc(BoundAggFunc {
            func: AggFunc::Sum,
            exprs: vec![build_bound_column_ref("t", "c1")],
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
            expr: build_bound_column_ref("t", "c1"),
            asc: false,
        }];
        LogicalOrder::new(order_by, input)
    }

    fn build_logical_joins() -> LogicalJoin {
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1 = t2.c1
        // left join t3 on t2.c1 = t3.c1 and t2.c1 > 1
        LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                Arc::new(build_logical_table_scan("t1")),
                Arc::new(build_logical_table_scan("t2")),
                JoinType::Inner,
                build_join_condition_eq("t1", "c1", "t2", "c1"),
            )),
            Arc::new(build_logical_table_scan("t3")),
            JoinType::Left,
            JoinCondition::On {
                on: vec![(
                    build_bound_column_ref("t2", "c1"),
                    build_bound_column_ref("t3", "c1"),
                )],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_column_ref_box("t2", "c1"),
                    right: build_int32_expr_box(1),
                    return_type: Some(DataType::Boolean),
                })),
            },
        )
    }

    #[test]
    fn test_rewrite_column_ref_to_input_ref() {
        let plan = build_logical_table_scan("t");
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
                left: build_bound_input_ref_box(0),
                right: build_int32_expr_box(2),
                return_type: Some(DataType::Boolean),
            })
        );
    }

    #[test]
    fn test_rewrite_column_ref_to_input_ref_when_join() {
        let plan = build_logical_joins();
        let project_plan = LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        );

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(project_plan));

        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs(),
            vec![
                build_bound_input_ref(0),
                build_bound_input_ref(2),
                build_bound_input_ref(4),
            ]
        );
        let child = &new_plan.children()[0];
        let join_plan = child.as_logical_join().unwrap();
        assert_matches!(join_plan.join_condition(), JoinCondition::On { .. });
        // to assert: left join t3 on t2.c1 = t3.c1 and t2.c1 > 1
        match join_plan.join_condition() {
            JoinCondition::On { on, filter } => {
                // every input_ref index is in spereate table
                assert_eq!(
                    on,
                    vec![(
                        // t2.c1 index is in left join table schema (t1.c1, t1.c2, t2.c1, t2.c2)
                        build_bound_input_ref(2),
                        // t3.c1 index is in right join table schema (t3.c1, t3.c2)
                        build_bound_input_ref(0)
                    )]
                );
                // filter(t2.c1 > 1) input_ref index is in last record_batch (left+right)
                assert_eq!(
                    filter,
                    Some(BoundExpr::BinaryOp(BoundBinaryOp {
                        op: BinaryOperator::Gt,
                        left: build_bound_input_ref_box(2),
                        right: build_int32_expr_box(1),
                        return_type: Some(DataType::Boolean),
                    }))
                );
            }
            _ => unreachable!(""),
        }
    }

    #[test]
    fn test_rewrite_simple_aggregation_column_ref_to_input_ref() {
        let plan = build_logical_table_scan("t");
        let plan = build_logical_project_with_simple_agg(Arc::new(plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(plan));

        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs(),
            vec![build_bound_input_ref(0)]
        );
    }

    #[test]
    fn test_rewrite_limit_to_input_ref() {
        let plan = build_logical_table_scan("t");
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
        let plan = build_logical_table_scan("t");
        let plan = build_logical_order(Arc::new(plan));

        let mut rewriter = InputRefRewriter::default();
        let new_plan = rewriter.rewrite(Arc::new(plan));

        assert_eq!(
            new_plan.as_logical_order().unwrap().order_by()[0],
            BoundOrderBy {
                expr: build_bound_input_ref(0),
                asc: false,
            }
        );
    }
}
