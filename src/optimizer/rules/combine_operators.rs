use super::util::is_subset_exprs;
use super::RuleImpl;
use crate::optimizer::core::{OptExpr, Pattern, PatternChildrenPredicate, Rule, Substitute};
use crate::optimizer::PlanNodeType;

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
