use super::batch::HepBatch;
use super::graph::{HepGraph, HepNodeId};
use super::matcher::HepMatcher;
use crate::optimizer::core::{PatternMatcher, Rule, Substitute};
use crate::optimizer::rules::RuleImpl;
use crate::optimizer::PlanRef;
use crate::util::pretty_plan_tree_string;

pub struct HepOptimizer {
    batches: Vec<HepBatch>,
    graph: HepGraph,
}

impl HepOptimizer {
    pub fn new(batches: Vec<HepBatch>, root: PlanRef) -> Self {
        let graph = HepGraph::new(root);
        Self { batches, graph }
    }

    pub fn find_best(&mut self) -> PlanRef {
        let batches = self.batches.clone().into_iter();
        for batch in batches {
            let mut iteration = 1_usize;
            // fixed_point means plan tree not changed after applying all rules.
            let mut fixed_point = false;
            // run until fix point or reach the max number of iterations as specified in the
            // strategy.
            while !fixed_point {
                println!("-----------------------------------------------------");
                println!("Start Batch: {}, iteration: {}", batch.name, iteration);

                fixed_point = self.apply_batch(&batch);

                // max_iteration check priority is higher than fixed_point.
                iteration += 1;
                if iteration > batch.strategy.max_iteration {
                    println!(
                        "Max iteration {} reached for batch: {}",
                        iteration - 1,
                        batch.name
                    );
                    break;
                }

                // if the plan tree not changed after applying all rules,
                // it reaches fix point, should stop.
                if fixed_point {
                    println!(
                        "Fixed point reached for batch: {}, after {} iterations",
                        batch.name,
                        iteration - 1
                    );
                    break;
                }
            }
        }
        self.graph.to_plan()
    }

    pub fn apply_batch(&mut self, batch: &HepBatch) -> bool {
        let original_plan = self.graph.to_plan();

        // for each rule will apply each node in graph.
        for rule in batch.rules.iter() {
            for node_id in self.graph.nodes_iter(batch.strategy.match_order) {
                if !self.apply_rule(rule.clone(), node_id) {
                    // not matched, will try next rule
                    continue;
                }

                println!(
                    "After apply plan tree:\n{}",
                    pretty_plan_tree_string(&*self.graph.to_plan())
                );

                // if the rule is applied, continue to try next rule in batch,
                // max_iteration only controls the iteration num of a batch.
                println!("Try next rule in batch ...");
                break;
            }
        }

        // Compare the two plan trees, if they are the same, it means the plan tree not changed
        let new_plan = self.graph.to_plan();
        let reach_fixed_point = original_plan == new_plan;
        println!(
            "Batch: {} finished, reach_fixed_point: {}",
            batch.name, reach_fixed_point,
        );
        reach_fixed_point
    }

    /// return true if the rule is applied which means the rule matched and the plan tree changed.
    fn apply_rule(&mut self, rule: RuleImpl, node_id: HepNodeId) -> bool {
        let matcher = HepMatcher::new(rule.pattern(), node_id, &self.graph);

        if let Some(opt_expr) = matcher.match_opt_expr() {
            let mut substitute = Substitute::default();
            let opt_expr_root = opt_expr.root.clone();
            rule.apply(opt_expr, &mut substitute);

            if !substitute.opt_exprs.is_empty() {
                assert!(substitute.opt_exprs.len() == 1);
                self.graph
                    .replace_node(node_id, substitute.opt_exprs[0].clone());
                println!(
                    "Apply {:?} at node {:?}: {:?}",
                    rule, node_id, opt_expr_root
                );
                return true;
            }
            // println!("Skip {:?} at node {:?}", rule, node_id);
            false
        } else {
            // println!("Skip {:?} at node {:?}", rule, node_id);
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::HepOptimizer;
    use crate::binder::test_util::*;
    use crate::binder::{BoundBinaryOp, BoundExpr};
    use crate::optimizer::{
        HepBatch, HepBatchStrategy, LogicalFilter, LogicalProject, LogicalTableScan,
        PhysicalRewriteRule, PlanRef,
    };

    fn build_logical_table_scan(table_id: &str) -> LogicalTableScan {
        LogicalTableScan::new(
            table_id.to_string(),
            None,
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
    #[test]
    fn test_hep_optimizer_works() {
        let plan = build_logical_table_scan("t");
        let filter_plan = build_logical_filter(Arc::new(plan));
        let project_plan = build_logical_project(Arc::new(filter_plan));
        let root = Arc::new(project_plan);
        let batch = HepBatch::new(
            "Final Step".to_string(),
            HepBatchStrategy::once_topdown(),
            vec![PhysicalRewriteRule::create()],
        );
        let mut planner = HepOptimizer::new(vec![batch], root);
        let new_plan = planner.find_best();
        assert_eq!(
            new_plan.as_physical_project().unwrap().logical().exprs()[0],
            build_bound_column_ref("t", "c2")
        );
    }
}
