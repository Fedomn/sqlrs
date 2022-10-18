use super::graph::{HepGraph, HepNodeId};
use crate::optimizer::core::{
    OptExpr, OptExprNode, Pattern, PatternChildrenPredicate, PatternMatcher,
};

/// Use pattern to determines which rule can be applied
pub struct HepMatcher<'a, 'b> {
    pub pattern: &'a Pattern,
    pub start_id: HepNodeId,
    pub graph: &'b HepGraph,
}

impl<'a, 'b> HepMatcher<'a, 'b> {
    pub fn new(pattern: &'a Pattern, start_id: HepNodeId, graph: &'b HepGraph) -> Self {
        Self {
            pattern,
            start_id,
            graph,
        }
    }
}

impl PatternMatcher for HepMatcher<'_, '_> {
    fn match_opt_expr(&self) -> Option<OptExpr> {
        let start_node = self.graph.node_plan(self.start_id);
        // check the root node predicate
        if !(self.pattern.predicate)(start_node) {
            return None;
        }
        // check the children's predicate
        let opt_expr = match &self.pattern.children {
            PatternChildrenPredicate::MatchedRecursive => self.graph.to_opt_expr(self.start_id),
            PatternChildrenPredicate::Predicate(children_patterns) => {
                let mut children_opt_exprs = vec![];
                for (idx, child_pattern) in children_patterns.iter().enumerate() {
                    // the predicates order should match the graph nodes order
                    let child_id = self.graph.children_at(self.start_id)[idx];
                    let m = HepMatcher::new(child_pattern, child_id, self.graph);
                    if let Some(opt_expr) = m.match_opt_expr() {
                        children_opt_exprs.push(opt_expr);
                    } else {
                        // if one of the children doesn't match, the whole pattern doesn't match
                        return None;
                    }
                }
                OptExpr {
                    // root need to regenerate due to rule may change its children
                    root: OptExprNode::PlanRef(self.graph.to_plan_start_from(self.start_id)),
                    children: children_opt_exprs,
                }
            }
            PatternChildrenPredicate::None => {
                // we don't care the children in rule logic, so it will collected as
                // OptExprNode::OptExpr in OptExpr tree.
                let children_opt_exprs = self
                    .graph
                    .children_at(self.start_id)
                    .into_iter()
                    .map(|id| OptExpr {
                        root: OptExprNode::OptExpr(id.index()),
                        children: vec![],
                    })
                    .collect::<Vec<_>>();
                OptExpr {
                    // FIXME: need to remove unnecessary planRef children when first generate graph,
                    // maybe could use dummy node
                    //
                    // root need to regenerate due to rule may change its children
                    root: OptExprNode::PlanRef(self.graph.to_plan_start_from(self.start_id)),
                    children: children_opt_exprs,
                }
            }
        };
        Some(opt_expr)
    }
}

#[cfg(test)]
mod tests {

    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::{BoundBinaryOp, BoundExpr, JoinCondition, JoinType};
    use crate::optimizer::{
        LogicalJoin, LogicalLimit, LogicalProject, LogicalTableScan, PlanNodeType,
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

    fn build_logical_joins() -> LogicalJoin {
        // matched sql:
        // select t1.c1, t2.c1, t3.c1 from t1
        // inner join t2 on t1.c1 = t2.c1
        // left join t3 on t2.c1 = t3.c1 and t2.c1 > 1
        LogicalJoin::new(
            Arc::new(LogicalJoin::new(
                Arc::new(build_logical_table_scan("t1")),
                Arc::new(build_logical_table_scan("t2")),
                JoinType::Inner,
                build_join_condition_eq("t1", "c1", "t2", "c1"),
            )),
            Arc::new(build_logical_table_scan("t3")),
            JoinType::Left,
            JoinCondition::On {
                on: vec![(
                    build_bound_column_ref("t2", "c1"),
                    build_bound_column_ref("t3", "c1"),
                )],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_column_ref_box("t2", "c1"),
                    right: build_int32_expr_box(1),
                    return_type: Some(DataType::Boolean),
                })),
            },
        )
    }

    fn build_graph() -> HepGraph {
        let join = build_logical_joins();
        let project = Arc::new(LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(join),
        ));
        let limit = Arc::new(LogicalLimit::new(
            Some(build_bound_constant(1)),
            Some(build_bound_constant(2)),
            project,
        ));
        HepGraph::new(limit)
    }

    #[test]
    fn test_match_opt_expr_with_children_predicate() {
        let graph = build_graph();

        // graph:
        //  0 <---------Limit{
        //     1 <--------Project {
        //       2 <----------Join {
        //          4 <-----------left: Join {
        //              6 <-----------t1,
        //              5 <-----------t2
        //                        },
        //          3 <-----------right: t3
        //                    }
        //                }
        //              }

        // pattern: Limit -> Project
        let pattern = Pattern {
            predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalLimit),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalProject),
                children: PatternChildrenPredicate::None,
            }]),
        };
        let start_id = HepNodeId::new(0);

        let m = HepMatcher::new(&pattern, start_id, &graph);
        let matched_opt_expr = m.match_opt_expr().unwrap();

        let limit_opt_expr = matched_opt_expr.root;
        assert_matches!(limit_opt_expr, OptExprNode::PlanRef(_));
        if let OptExprNode::PlanRef(p) = limit_opt_expr {
            assert_eq!(p.node_type(), PlanNodeType::LogicalLimit);
        }

        let project_opt_expr = matched_opt_expr.children[0].root.clone();
        assert_matches!(project_opt_expr, OptExprNode::PlanRef(_));
        if let OptExprNode::PlanRef(p) = project_opt_expr {
            assert_eq!(p.node_type(), PlanNodeType::LogicalProject);
        }

        let join_opt_expr = matched_opt_expr.children[0].children[0].root.clone();
        assert_matches!(join_opt_expr, OptExprNode::OptExpr(_));
        if let OptExprNode::OptExpr(o) = join_opt_expr {
            assert_eq!(o, 2);
        }
    }

    #[test]
    fn test_match_opt_expr_with_unmatched_children_predicate() {
        let graph = build_graph();

        // graph:
        //  0 <---------Limit{
        //     1 <--------Project {
        //       2 <----------Join {
        //          4 <-----------left: Join {
        //              6 <-----------t1,
        //              5 <-----------t2
        //                        },
        //          3 <-----------right: t3
        //                    }
        //                }
        //              }

        // pattern: Limit -> Project
        let pattern = Pattern {
            predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalLimit),
            children: PatternChildrenPredicate::Predicate(vec![Pattern {
                predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalLimit),
                children: PatternChildrenPredicate::None,
            }]),
        };
        let start_id = HepNodeId::new(0);

        let m = HepMatcher::new(&pattern, start_id, &graph);
        assert!(m.match_opt_expr().is_none());
    }

    #[test]
    fn test_match_opt_expr_with_children_predicate_matched_recursive() {
        let graph = build_graph();

        // graph:
        //  0 <---------Limit{
        //     1 <--------Project {
        //       2 <----------Join {
        //          4 <-----------left: Join {
        //              6 <-----------t1,
        //              5 <-----------t2
        //                        },
        //          3 <-----------right: t3
        //                    }
        //                }
        //              }

        // pattern: Limit -> Project
        let pattern = Pattern {
            predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalLimit),
            children: PatternChildrenPredicate::MatchedRecursive,
        };
        let start_id = HepNodeId::new(0);

        let m = HepMatcher::new(&pattern, start_id, &graph);
        let matched_opt_expr = m.match_opt_expr().unwrap();
        let limit = matched_opt_expr.root;
        assert_matches!(limit, OptExprNode::PlanRef(_));
        let project = matched_opt_expr.children[0].root.clone();
        assert_matches!(project, OptExprNode::PlanRef(_));
        let join = matched_opt_expr.children[0].children[0].root.clone();
        assert_matches!(join, OptExprNode::PlanRef(_));
        let left_table = matched_opt_expr.children[0].children[0].children[0]
            .root
            .clone();
        assert_matches!(left_table, OptExprNode::PlanRef(_));
    }

    #[test]
    fn test_match_opt_expr_with_children_predicate_none() {
        let graph = build_graph();

        // graph:
        //  0 <---------Limit{
        //     1 <--------Project {
        //       2 <----------Join {
        //          4 <-----------left: Join {
        //              6 <-----------t1,
        //              5 <-----------t2
        //                        },
        //          3 <-----------right: t3
        //                    }
        //                }
        //              }

        // pattern: Limit -> Project
        let pattern = Pattern {
            predicate: |plan| matches!(plan.node_type(), PlanNodeType::LogicalLimit),
            children: PatternChildrenPredicate::None,
        };
        let start_id = HepNodeId::new(0);

        let m = HepMatcher::new(&pattern, start_id, &graph);
        let matched_opt_expr = m.match_opt_expr().unwrap();
        let limit = matched_opt_expr.root;
        assert_matches!(limit, OptExprNode::PlanRef(_));
        let project = matched_opt_expr.children[0].root.clone();
        assert_matches!(project, OptExprNode::OptExpr(1));
    }
}
