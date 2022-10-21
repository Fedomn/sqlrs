use std::sync::Arc;

use super::RuleImpl;
use crate::binder::{BoundExpr, JoinCondition};
use crate::optimizer::core::{OptExpr, Pattern, PatternChildrenPredicate, Rule, Substitute};
use crate::optimizer::expr_rewriter::ExprRewriter;
use crate::optimizer::{
    LogicalAgg, LogicalFilter, LogicalJoin, LogicalLimit, LogicalOrder, LogicalProject, PlanRef,
    PlanRewriter,
};
use crate::planner::PlannerContext;

lazy_static! {
    static ref SIMPLIFY_CASTS_RULE: Pattern = {
        Pattern {
            predicate: |p| p.is_logical_plan(),
            children: PatternChildrenPredicate::MatchedRecursive,
        }
    };
}

#[derive(Clone)]
pub struct SimplifyCasts;

impl SimplifyCasts {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for SimplifyCasts {
    fn pattern(&self) -> &Pattern {
        &SIMPLIFY_CASTS_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute, _planner_context: &PlannerContext) {
        let mut rewriter = SimplifyCastsRewriter::default();
        let plan = opt_expr.to_plan_ref();
        let new_plan = rewriter.rewrite(plan);

        let res = OptExpr::new_from_plan_ref(&new_plan);
        result.opt_exprs.push(res);
    }
}

#[derive(Default)]
pub struct SimplifyCastsRewriter;

impl ExprRewriter for SimplifyCastsRewriter {
    fn rewrite_type_cast(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::TypeCast(bound_cast) => {
                let casted_expr = bound_cast.expr.clone();
                if casted_expr.return_type().unwrap() == bound_cast.cast_type {
                    *expr = *casted_expr;
                    return;
                }
                if let BoundExpr::Constant(scalar_value) = *casted_expr {
                    if let Some(cast_val) = scalar_value.cast_to_type(&bound_cast.cast_type) {
                        *expr = BoundExpr::Constant(cast_val);
                    }
                }
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_binary_op(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::BinaryOp(e) => {
                self.rewrite_expr(&mut e.left);
                self.rewrite_expr(&mut e.right);
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_agg_func(&self, expr: &mut BoundExpr) {
        match expr {
            BoundExpr::AggFunc(e) => {
                for arg in &mut e.exprs {
                    self.rewrite_expr(arg);
                }
            }
            _ => unreachable!(),
        }
    }
}

// TODO: extract expr rewrite common logic since it's almost duplicated with InputRefRewriter
impl PlanRewriter for SimplifyCastsRewriter {
    fn rewrite_logical_project(&mut self, plan: &LogicalProject) -> PlanRef {
        let new_child = self.rewrite(plan.input());

        let mut new_exprs = plan.exprs();
        for expr in &mut new_exprs {
            self.rewrite_expr(expr);
        }

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

    fn rewrite_logical_agg(&mut self, plan: &LogicalAgg) -> PlanRef {
        let new_child = self.rewrite(plan.input());

        let mut new_agg_funcs = plan.agg_funcs();
        for expr in &mut new_agg_funcs {
            self.rewrite_expr(expr);
        }

        let mut new_group_exprs = plan.group_by();
        for expr in &mut new_group_exprs {
            self.rewrite_expr(expr);
        }

        let new_plan = LogicalAgg::new(new_agg_funcs, new_group_exprs, new_child);
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

    fn rewrite_logical_join(&mut self, plan: &LogicalJoin) -> PlanRef {
        let original_join_output_columns = plan.join_output_columns();

        let new_left = self.rewrite(plan.left());
        let mut right_rewriter = SimplifyCastsRewriter::default();
        let new_right = right_rewriter.rewrite(plan.right());

        let new_on = if let JoinCondition::On { on, filter: _ } = plan.join_condition() {
            let mut on_left_keys = on.iter().map(|o| o.0.clone()).collect::<Vec<_>>();
            let mut on_right_keys = on.iter().map(|o| o.1.clone()).collect::<Vec<_>>();

            for expr in &mut on_left_keys {
                self.rewrite_expr(expr);
            }

            for expr in &mut on_right_keys {
                right_rewriter.rewrite_expr(expr);
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
}
