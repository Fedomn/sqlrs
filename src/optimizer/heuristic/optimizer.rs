use super::graph::{HepGraph, HepNodeId};
use super::matcher::HepMatcher;
use super::program::{HepInstruction, HepProgram};
use crate::optimizer::core::{PatternMatcher, Rule, Substitute};
use crate::optimizer::rules::RuleImpl;
use crate::optimizer::PlanRef;

pub struct HepOptimizer {
    program: HepProgram,
    graph: HepGraph,
}

impl HepOptimizer {
    pub fn new(program: HepProgram, root: PlanRef) -> Self {
        let graph = HepGraph::new(root);
        Self { program, graph }
    }

    pub fn find_best(&mut self) -> PlanRef {
        for ins in self.program.instructions.clone().iter() {
            match ins {
                HepInstruction::Rule(rule) => {
                    self.apply_rules(vec![rule.clone()]);
                }
                HepInstruction::Rules(rules) => {
                    self.apply_rules(rules.clone());
                }
                HepInstruction::MatchOrder(match_order) => {
                    self.program.state.match_order = *match_order;
                }
                HepInstruction::MatchLimit(match_limit) => {
                    self.program.state.match_limit = *match_limit;
                }
            }
        }
        self.graph.to_plan()
    }

    fn apply_rules(&mut self, rules: Vec<RuleImpl>) {
        let mut match_cnt = 0;
        let mut iter = self.graph.nodes_iter(self.program.state.match_order);
        while let Some(node_id) = iter.next() {
            // for each node in the graph will apply each rule
            for rule in rules.iter() {
                if !self.apply_rule(rule.clone(), node_id) {
                    // not matched, will try next rule
                    continue;
                }
                match_cnt += 1;
                if match_cnt >= self.program.state.match_limit {
                    println!("match limit reached {}", match_cnt);
                    return;
                }

                // if a rule applied successfully, the planner will restart from new root
                iter = self.graph.nodes_iter(self.program.state.match_order);
                break;
            }
        }
    }

    fn apply_rule(&mut self, rule: RuleImpl, node_id: HepNodeId) -> bool {
        let matcher = HepMatcher::new(rule.pattern(), node_id, &self.graph);

        // println!("before graph: {:#?}", self.graph);
        if let Some(opt_expr) = matcher.match_opt_expr() {
            println!("match rule {:?}", rule);
            // println!("match rule {:?} and opt_expr: {:#?}", rule, opt_expr);
            let mut substitute = Substitute::default();
            rule.apply(opt_expr, &mut substitute);

            if !substitute.opt_exprs.is_empty() {
                assert!(substitute.opt_exprs.len() == 1);
                self.graph
                    .replace_node(node_id, substitute.opt_exprs[0].clone());
            }
            true
        } else {
            println!("skip rule: {:?}", rule);
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
    use crate::optimizer::heuristic::program::{HepInstruction, HepMatchOrder, HepProgram};
    use crate::optimizer::rules::InputRefRwriteRule;
    use crate::optimizer::{LogicalFilter, LogicalProject, LogicalTableScan, PlanRef};

    fn build_logical_table_scan(table_id: &str) -> LogicalTableScan {
        LogicalTableScan::new(
            table_id.to_string(),
            vec![
                build_column_catalog(table_id, "c1"),
                build_column_catalog(table_id, "c2"),
            ],
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
    fn test_hep_optimizer_program_works() {
        let plan = build_logical_table_scan("t");
        let filter_plan = build_logical_filter(Arc::new(plan));
        let project_plan = build_logical_project(Arc::new(filter_plan));
        let root = Arc::new(project_plan);
        let program = HepProgram::new(vec![
            HepInstruction::MatchOrder(HepMatchOrder::TopDown),
            HepInstruction::MatchOrder(HepMatchOrder::BottomUp),
            HepInstruction::MatchLimit(10),
            HepInstruction::MatchOrder(HepMatchOrder::TopDown),
            HepInstruction::MatchLimit(2),
            HepInstruction::Rule(InputRefRwriteRule::create()),
        ]);
        let mut planner = HepOptimizer::new(program, root);
        let new_plan = planner.find_best();
        assert_eq!(
            new_plan.as_logical_project().unwrap().exprs()[0],
            build_bound_input_ref(1)
        );
    }
}
