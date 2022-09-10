use std::sync::Arc;

use super::RuleImpl;
use crate::optimizer::core::{
    OptExpr, OptExprNode, Pattern, PatternChildrenPredicate, Rule, Substitute,
};
use crate::optimizer::{LogicalTableScan, PlanNodeType};

lazy_static! {
    static ref PUSH_PROJECT_INTO_TABLE_SCAN_RULE: Pattern = {
        Pattern {
            predicate: |p| p.node_type() == PlanNodeType::LogicalProject,
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |p| p.node_type() == PlanNodeType::LogicalTableScan,
                children: PatternChildrenPredicate::None,
            }]),
        }
    };
}

#[derive(Clone)]
pub struct PushProjectIntoTableScan;

impl PushProjectIntoTableScan {
    pub fn create() -> RuleImpl {
        Self {}.into()
    }
}

impl Rule for PushProjectIntoTableScan {
    fn pattern(&self) -> &Pattern {
        &PUSH_PROJECT_INTO_TABLE_SCAN_RULE
    }

    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute) {
        let project_opt_expr_root = opt_expr.root;
        let table_scan_opt_expr = opt_expr.children[0].clone();
        let project_node = project_opt_expr_root
            .get_plan_ref()
            .as_logical_project()
            .unwrap();
        let table_scan_node = table_scan_opt_expr
            .root
            .get_plan_ref()
            .as_logical_table_scan()
            .unwrap();

        let columns = project_node
            .exprs()
            .iter()
            .flat_map(|e| e.get_column_catalog())
            .collect::<Vec<_>>();
        let original_columns = table_scan_node.columns();
        let projections = columns
            .iter()
            .map(|c| original_columns.iter().position(|oc| oc == c).unwrap())
            .collect::<Vec<_>>();

        let new_table_scan_node = LogicalTableScan::new(
            table_scan_node.table_id(),
            columns,
            table_scan_node.bounds(),
            Some(projections),
        );

        let new_table_scan_opt_expr = OptExpr::new(
            OptExprNode::PlanRef(Arc::new(new_table_scan_node)),
            table_scan_opt_expr.children,
        );

        result.opt_exprs.push(new_table_scan_opt_expr);
    }
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use crate::optimizer::rules::rule_test_util::{build_plan, RuleTest};
    use crate::optimizer::{HepBatch, HepBatchStrategy, HepOptimizer, PushProjectIntoTableScan};
    use crate::util::pretty_plan_tree_string;

    #[test]
    fn test_push_project_into_table_scan_rule() {
        let tests = vec![RuleTest {
            name: "push_project_into_table_scan_rule",
            sql: "select a from t1",
            expect: "LogicalTableScan: table: #t1, columns: [a]",
        }];

        for t in tests {
            let logical_plan = build_plan(t.sql);
            let batch = HepBatch::new(
                "Column Pruning".to_string(),
                HepBatchStrategy::fix_point_topdown(100),
                vec![PushProjectIntoTableScan::create()],
            );
            let mut optimizer = HepOptimizer::new(vec![batch], logical_plan);

            let optimized_plan = optimizer.find_best();

            let l = t.expect.trim_start();
            let r = pretty_plan_tree_string(&*optimized_plan);
            assert_eq!(l, r.trim_end(), "actual plan:\n{}", r);
        }
    }
}
