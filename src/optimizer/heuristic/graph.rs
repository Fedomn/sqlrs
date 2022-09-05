use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::Bfs;

use super::HepMatchOrder;
use crate::optimizer::core::{OptExpr, OptExprNode, OptExprNodeId};
use crate::optimizer::PlanRef;

/// HepNodeId is used in optimizer to identify a node.
pub type HepNodeId = NodeIndex<OptExprNodeId>;

#[derive(Clone, Debug)]
pub struct HepNode {
    id: HepNodeId,
    plan: PlanRef,
}

#[derive(Debug)]
pub struct HepGraph {
    graph: StableDiGraph<HepNode, (), usize>,
    root: HepNodeId,
}

impl HepGraph {
    pub fn new(root: PlanRef) -> Self {
        let mut graph = Self {
            graph: StableDiGraph::<HepNode, (), usize>::default(),
            root: HepNodeId::default(),
        };
        let opt_expr = OptExpr::new_from_plan_ref(&root);
        graph.root = graph.add_opt_expr(opt_expr);
        graph
    }

    pub fn children_at(&self, id: HepNodeId) -> Vec<HepNodeId> {
        self.graph
            .neighbors_directed(id, petgraph::Direction::Outgoing)
            .collect::<Vec<_>>()
            .into_iter()
            .collect()
    }

    /// DFS visitor to add a Optimizer Expression in graph and reactify the graph edges.
    fn add_opt_expr(&mut self, opt_expr: OptExpr) -> HepNodeId {
        let root = opt_expr.root.clone();
        match root {
            // the optimizer expression contains existing graph node, so just return the node id.
            OptExprNode::OptExpr(id) => HepNodeId::new(id),
            // the optimizer expression is a new graph nodes
            OptExprNode::PlanRef(root) => {
                let root_hep_node = HepNode {
                    // fake id for now, will be updated after add_node
                    id: HepNodeId::default(),
                    plan: root.clone(),
                };
                let new_node_id = self.graph.add_node(root_hep_node);
                self.graph[new_node_id].id = new_node_id;

                // The rev() operation to reverse the children order in graph. Due to
                // neighbors_directed Outgoing returns nodes order is reversed.
                //
                // For example, if the node is join, when insert children order is [left, right],
                // then neighbors_directed Outgoing will return [right, left].
                //
                // So we should reverse order when insert to make sure the neighbors_directed
                // children order is [left, right], and the order only works in TopDown, because
                // BottomUp will reverse all ids.
                let children_ids = opt_expr
                    .children
                    .into_iter()
                    .rev()
                    .map(|p| self.add_opt_expr(p))
                    .collect::<Vec<_>>();

                for child_hep_id in children_ids {
                    self.graph.add_edge(new_node_id, child_hep_id, ());
                }

                new_node_id
            }
        }
    }

    /// Convert the graph to a plan tree, recursively process children and construct new plan.
    pub fn to_plan(&self) -> PlanRef {
        self.to_plan_internal(self.root)
    }

    fn to_plan_internal(&self, start: HepNodeId) -> PlanRef {
        let ids = self.children_at(start);

        // recursively process children's children
        let children = ids
            .iter()
            .map(|&id| self.to_plan_internal(id))
            .collect::<Vec<_>>();
        self.graph[start].plan.clone_with_children(children)
    }

    pub fn to_opt_expr(&self, start: HepNodeId) -> OptExpr {
        let children = self
            .children_at(start)
            .iter()
            .map(|&id| self.to_opt_expr(id))
            .collect::<Vec<_>>();
        OptExpr::new(
            OptExprNode::PlanRef(self.graph[start].plan.clone()),
            children,
        )
    }

    /// Traverse the graph in BFS order.
    fn bfs(&self, start: HepNodeId) -> Vec<HepNodeId> {
        let mut ids = Vec::with_capacity(self.graph.node_count());
        let mut iter = Bfs::new(&self.graph, start);
        while let Some(node_id) = iter.next(&self.graph) {
            ids.push(node_id);
        }
        ids
    }

    pub fn nodes_iter(&self, order: HepMatchOrder) -> Box<dyn Iterator<Item = HepNodeId>> {
        let ids = self.bfs(self.root);
        match order {
            HepMatchOrder::TopDown => Box::new(ids.into_iter()),
            HepMatchOrder::BottomUp => Box::new(ids.into_iter().rev()),
        }
    }

    pub fn node_plan(&self, id: HepNodeId) -> &PlanRef {
        &self.graph[id].plan
    }

    pub fn replace_node(&mut self, old_node_id: HepNodeId, new_opt_expr: OptExpr) {
        // add new node and rectify edges with existing children nodes
        let new_node_id = self.add_opt_expr(new_opt_expr);

        // change replaced node's parents point to new child
        let parent_ids = self
            .graph
            .neighbors_directed(old_node_id, petgraph::Direction::Incoming)
            .collect::<Vec<_>>();
        for parent_id in parent_ids {
            self.graph.add_edge(parent_id, new_node_id, ());
        }
        // remove old node
        self.graph.remove_node(old_node_id);

        if self.root == old_node_id {
            self.root = new_node_id;
        }

        // remove unlink nodes from root
        let ids_in_plan_tree = self.bfs(self.root);
        if self.graph.node_count() != ids_in_plan_tree.len() {
            self.graph
                .retain_nodes(|_, id| ids_in_plan_tree.contains(&id));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use petgraph::Direction;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::{BoundBinaryOp, BoundExpr, BoundOrderBy, JoinCondition, JoinType};
    use crate::optimizer::{
        Dummy, LogicalJoin, LogicalOrder, LogicalProject, LogicalTableScan, PlanNodeType,
        PlanTreeNode,
    };

    fn build_logical_table_scan(table_id: &str) -> LogicalTableScan {
        LogicalTableScan::new(
            table_id.to_string(),
            vec![
                build_column_catalog(table_id, "c1"),
                build_column_catalog(table_id, "c2"),
            ],
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

    #[test]
    fn test_graph_add_plan_ref() {
        let plan = build_logical_joins();
        let project_plan = Arc::new(LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        ));

        let graph = HepGraph::new(project_plan);
        assert_eq!(graph.root, HepNodeId::new(0));

        // graph:
        // 0 <--------Project {
        //   1 <----------Join {
        //      3 <-----------left: Join {
        //          5 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      2 <-----------right: t3
        //                }
        //            }
        let node_ids = graph
            .graph
            .neighbors_directed(HepNodeId::new(1), Direction::Outgoing)
            .collect::<Vec<_>>();
        assert_eq!(node_ids, vec![HepNodeId::new(3), HepNodeId::new(2)]);

        let node_ids = graph
            .graph
            .neighbors_directed(HepNodeId::new(3), Direction::Outgoing)
            .collect::<Vec<_>>();
        assert_eq!(node_ids, vec![HepNodeId::new(5), HepNodeId::new(4)]);

        let node_ids = graph
            .graph
            .neighbors_directed(HepNodeId::new(0), Direction::Outgoing)
            .collect::<Vec<_>>();
        assert_eq!(node_ids, vec![HepNodeId::new(1)]);
    }

    #[test]
    fn test_graph_nodes_iter() {
        let plan = build_logical_joins();
        let project_plan = LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        );

        let graph = HepGraph::new(Arc::new(project_plan));
        // graph:
        // 0 <--------Project {
        //   1 <----------Join {
        //      3 <-----------left: Join {
        //          5 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      2 <-----------right: t3
        //                }
        //            }

        // down-top returned join children order is [right, left]
        let bottom_up_ids = graph
            .nodes_iter(HepMatchOrder::BottomUp)
            .collect::<Vec<_>>();
        assert_eq!(
            bottom_up_ids,
            vec![
                HepNodeId::new(4),
                HepNodeId::new(5),
                HepNodeId::new(2),
                HepNodeId::new(3),
                HepNodeId::new(1),
                HepNodeId::new(0),
            ]
        );

        // top-down returned join children order is [left, right]
        let top_down_ids = graph.nodes_iter(HepMatchOrder::TopDown).collect::<Vec<_>>();
        assert_eq!(
            top_down_ids,
            vec![
                HepNodeId::new(0),
                HepNodeId::new(1),
                HepNodeId::new(3),
                HepNodeId::new(2),
                HepNodeId::new(5),
                HepNodeId::new(4),
            ]
        );
    }

    #[test]
    fn test_graph_children_at() {
        let plan = build_logical_joins();
        let project_plan = LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        );

        let graph = HepGraph::new(Arc::new(project_plan));

        let ids = graph.children_at(HepNodeId::new(3));
        assert_eq!(ids, vec![HepNodeId::new(5), HepNodeId::new(4)]);
    }

    #[test]
    fn test_graph_to_plan() {
        let plan = build_logical_joins();
        let project_plan = LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        );

        let graph = HepGraph::new(Arc::new(project_plan));
        let new_plan = graph.to_plan();
        let project_child = new_plan.children()[0].clone();
        let join = project_child.as_logical_join().unwrap();
        let left = join.left();
        let nested_join = left.as_logical_join().unwrap();

        assert_eq!(
            nested_join
                .left()
                .as_logical_table_scan()
                .unwrap()
                .table_id(),
            "t1"
        );
        assert_eq!(
            nested_join
                .right()
                .as_logical_table_scan()
                .unwrap()
                .table_id(),
            "t2"
        );
        assert_eq!(
            join.right().as_logical_table_scan().unwrap().table_id(),
            "t3"
        );
    }

    #[test]
    fn test_graph_replace_node_and_remove_unlink_nodes() {
        let table_scan = Arc::new(build_logical_table_scan("t1"));
        let project_plan = Arc::new(LogicalProject::new(
            vec![build_bound_column_ref("t1", "c1")],
            table_scan.clone(),
        ));
        let mut graph = HepGraph::new(project_plan);
        let new_opt_expr = OptExpr {
            root: OptExprNode::PlanRef(Arc::new(LogicalProject::new(
                vec![build_bound_input_ref(0)],
                Dummy::new_ref(),
            ))),
            children: vec![OptExpr {
                root: OptExprNode::PlanRef(table_scan),
                children: vec![],
            }],
        };
        let original_nodes = graph.nodes_iter(HepMatchOrder::TopDown).collect::<Vec<_>>();

        graph.replace_node(HepNodeId::new(0), new_opt_expr);

        for node in original_nodes {
            assert!(!graph.nodes_iter(HepMatchOrder::TopDown).any(|x| x == node));
        }
    }

    #[test]
    fn test_graph_replace_node() {
        let plan = build_logical_joins();
        let project_plan = Arc::new(LogicalProject::new(
            vec![
                build_bound_column_ref("t1", "c1"),
                build_bound_column_ref("t2", "c1"),
                build_bound_column_ref("t3", "c1"),
            ],
            Arc::new(plan),
        ));

        // graph:
        // 0 <--------Project {
        //   1 <----------Join {
        //      3 <-----------left: Join {
        //          5 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      2 <-----------right: t3
        //                }
        //            }
        let mut graph = HepGraph::new(project_plan.clone());

        // we assume that have a rule will matched LogicalProject, and it will change LogicalProject
        // to LogicalOrder+LogicalProject, and origial LogicalProject's children will reconnect to
        // new node.

        let logical_order_opt_expr = OptExprNode::PlanRef(Arc::new(LogicalOrder::new(
            vec![BoundOrderBy {
                expr: build_bound_column_ref("t1", "c1"),
                asc: false,
            }],
            Dummy::new_ref(),
        )));
        let logical_project_opt_expr =
            OptExprNode::PlanRef(project_plan.clone_with_children(vec![Dummy::new_ref()]));
        let existing_node_id = OptExprNode::OptExpr(1);
        let new_opt_expr = OptExpr {
            root: logical_order_opt_expr,
            children: vec![OptExpr {
                root: logical_project_opt_expr,
                children: vec![OptExpr {
                    root: existing_node_id,
                    children: vec![],
                }],
            }],
        };
        graph.replace_node(HepNodeId::new(0), new_opt_expr);

        // after replace graph:
        // 6 <--------Order {
        //     7 <--------Project {
        //       1 <----------Join {
        //          3 <-----------left: Join {
        //              5 <-----------t1,
        //              4 <-----------t2
        //                        },
        //          2 <-----------right: t3
        //                    }
        //                }
        //            }
        let result = graph.to_plan();
        assert_eq!(result.node_type(), PlanNodeType::LogicalOrder);
        assert_eq!(graph.root, HepNodeId::new(6));
        let project_id = graph.children_at(HepNodeId::new(6))[0];
        let join_id = graph.children_at(project_id)[0];
        assert_eq!(join_id, HepNodeId::new(1));
    }
}
