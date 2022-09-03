use super::RuleImpl;
use crate::optimizer::core::*;
use crate::optimizer::{InputRefRewriter, PlanRewriter};

lazy_static! {
    static ref PATTERN: Pattern = {
        Pattern {
            predicate: |p| p.contains_column_ref_expr(),
            children: PatternChildrenPredicate::MatchedRecursive,
        }
    };
}

#[derive(Clone)]
pub struct InputRefRwriteRule;

impl InputRefRwriteRule {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for InputRefRwriteRule {
    fn pattern(&self) -> &Pattern {
        &PATTERN
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let mut rewriter = InputRefRewriter::default();
        let plan = opt_expr.to_plan_ref();

        let new_plan = rewriter.rewrite(plan);

        let res = OptExpr::new_from_plan_ref(&new_plan);
        result.opt_exprs.push(res);
    }
}
