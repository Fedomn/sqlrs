use std::sync::Arc;

use super::util::{is_subset_exprs, reduce_conjunctive_predicate};
use super::RuleImpl;
use crate::optimizer::core::{
    OptExpr, OptExprNode, Pattern, PatternChildrenPredicate, Rule, Substitute,
};
use crate::optimizer::{Dummy, LogicalFilter, PlanNodeType};

lazy_static! {
    static ref COLLAPSE_PROJECT_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
    static ref COMBINE_FILTERS: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalFilter,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalFilter,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

/// Combine two adjacent project operators into one.
#[derive(Clone)]
pub struct CollapseProject;

impl CollapseProject {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for CollapseProject {
    fn pattern(&self) -> &Pattern {
        &COLLAPSE_PROJECT_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        // TODO: handle column alias
        let project_opt_expr = opt_expr;
        let next_project_opt_expr = project_opt_expr.children[0].clone();

        let project_exprs = project_opt_expr
            .root
            .get_plan_ref()
            .as_logical_project()
            .unwrap()
            .exprs();
        let next_project_exprs = next_project_opt_expr
            .root
            .get_plan_ref()
            .as_logical_project()
            .unwrap()
            .exprs();
        if is_subset_exprs(&project_exprs, &next_project_exprs) {
            let res = OptExpr::new(project_opt_expr.root, next_project_opt_expr.children);
            result.opt_exprs.push(res);
        }
    }
}

/// Combine two adjacent filter operators into one.
#[derive(Clone)]
pub struct CombineFilter;

impl CombineFilter {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for CombineFilter {
    fn pattern(&self) -> &Pattern {
        &COMBINE_FILTERS
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        // TODO: handle column alias
        let filter_opt_expr = opt_expr;
        let next_filter_opt_expr = filter_opt_expr.children[0].clone();

        let filter_expr = filter_opt_expr
            .root
            .get_plan_ref()
            .as_logical_filter()
            .unwrap()
            .expr();
        let next_filter_exprs = next_filter_opt_expr
            .root
            .get_plan_ref()
            .as_logical_filter()
            .unwrap()
            .expr();
        if let Some(expr) = reduce_conjunctive_predicate([filter_expr, next_filter_exprs].to_vec())
        {
            let new_filter_root =
                OptExprNode::PlanRef(Arc::new(LogicalFilter::new(expr, Dummy::new_ref())));
            let res = OptExpr::new(new_filter_root, next_filter_opt_expr.children);
            result.opt_exprs.push(res);
        }
    }
}
