use super::RuleImpl;
use crate::optimizer::core::{OptExpr, Pattern, PatternChildrenPredicate, Rule, Substitute};
use crate::optimizer::PlanNodeType;

lazy_static! {
    static ref LIMIT_PROJECT_TRANSPOSE_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalLimit,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

#[derive(Clone)]
pub struct LimitProjectTranspose;

impl LimitProjectTranspose {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for LimitProjectTranspose {
    fn pattern(&self) -> &Pattern {
        &LIMIT_PROJECT_TRANSPOSE_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let limit_opt_expr_root = opt_expr.root;
        let project_opt_expr = opt_expr.children[0].clone();

        let new_project_opt_expr = OptExpr::new(
            project_opt_expr.root,
            vec![OptExpr::new(limit_opt_expr_root, project_opt_expr.children)],
        );

        result.opt_exprs.push(new_project_opt_expr);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::optimizer::rules::rule_test_util::{build_plan, RuleTest};
    use crate::optimizer::{HepBatch, HepBatchStrategy, HepOptimizer, LimitProjectTranspose};
    use crate::util::pretty_plan_tree_string;

    #[test]
    fn test_limit_project_transpose_rule() {
        let tests = vec![RuleTest {
            name: "limit_project_transpose_rule",
            sql: "select a from t1 offset 2 limit 1",
            expect: r"
LogicalProject: exprs [t1.a:Nullable(Int32)]
  LogicalLimit: limit Some(1), offset Some(2)
    LogicalTableScan: table: #t1, columns: [a, b, c]",
        }];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Operator push down".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![LimitProjectTranspose::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }
}
