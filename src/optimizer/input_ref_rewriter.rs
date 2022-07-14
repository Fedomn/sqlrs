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
        // TODO: implement this.
        Arc::new(plan.clone())
    }
}
