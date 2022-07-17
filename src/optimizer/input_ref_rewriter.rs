use std::sync::Arc;

use super::expr_rewriter::ExprRewriter;
use super::{LogicalFilter, LogicalProject, LogicalTableScan, PlanRef, PlanRewriter};
use crate::binder::{BoundColumnRef, BoundExpr, BoundInputRef};

#[derive(Default)]
pub struct InputRefRewriter {
    /// The bound exprs of the last visited plan node, which is used to resolve the index of
    /// RecordBatch.
    bindings: Vec<BoundExpr>,
}

impl ExprRewriter for InputRefRewriter {
    fn rewrite_column_ref(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::ColumnRef(_) => {
                if let Some(idx) = self.bindings.iter().position(|e| *e == expr.clone()) {
                    *expr = BoundExpr::InputRef(BoundInputRef {
                        index: idx,
                        return_type: expr.return_type().unwrap(),
                    });
                }
            }
            _ => unreachable!(),
        }
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
}

#[cfg(test)]
mod input_ref_rewriter_test {
    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::BoundBinaryOp;
    use crate::catalog::{ColumnCatalog, ColumnDesc};
    use crate::types::ScalarValue;

    fn build_test_column(column_name: String) -> ColumnCatalog {
        ColumnCatalog {
            id: column_name.clone(),
            desc: ColumnDesc {
                name: column_name,
                data_type: DataType::Int32,
            },
        }
    }

    fn build_logical_table_scan() -> LogicalTableScan {
        LogicalTableScan::new(
            "t".to_string(),
            vec![
                build_test_column("c1".to_string()),
                build_test_column("c2".to_string()),
            ],
        )
    }

    fn build_logical_project(input: PlanRef) -> LogicalProject {
        LogicalProject::new(
            vec![BoundExpr::ColumnRef(BoundColumnRef {
                column_catalog: build_test_column("c2".to_string()),
            })],
            input,
        )
    }

    fn build_logical_filter(input: PlanRef) -> LogicalFilter {
        LogicalFilter::new(
            BoundExpr::BinaryOp(BoundBinaryOp {
                op: BinaryOperator::Eq,
                left: Box::new(BoundExpr::ColumnRef(BoundColumnRef {
                    column_catalog: build_test_column("c1".to_string()),
                })),
                right: Box::new(BoundExpr::Constant(ScalarValue::Int32(Some(2)))),
                return_type: Some(DataType::Boolean),
            }),
            input,
        )
    }

    #[test]
    fn test_rewrite_column_ref_to_input_ref() {
        let plan = build_logical_table_scan();
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
}
