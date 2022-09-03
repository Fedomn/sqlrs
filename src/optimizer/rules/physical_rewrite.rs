use super::RuleImpl;
use crate::optimizer::core::*;
use crate::optimizer::{PhysicalRewriter, PlanRewriter};

lazy_static! {
    static ref PATTERN: Pattern = {
        Pattern {
            predicate: |p| p.is_logical_plan(),
            children: PatternChildrenPredicate::MatchedRecursive,
        }
    };
}

#[derive(Clone)]
pub struct PhysicalRewriteRule;

impl PhysicalRewriteRule {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PhysicalRewriteRule {
    fn pattern(&self) -> &Pattern {
        &PATTERN
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let mut rewriter = PhysicalRewriter::default();
        let plan = opt_expr.to_plan_ref();
        let new_plan = rewriter.rewrite(plan);

        let res = OptExpr::new_from_plan_ref(&new_plan);
        result.opt_exprs.push(res);
    }
}
