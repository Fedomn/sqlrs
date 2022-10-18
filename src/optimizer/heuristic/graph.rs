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
    graph: StableDiGraph<HepNode, usize, usize>,
    root: HepNodeId,
}

impl HepGraph {
    pub fn new(root: PlanRef) -> Self {
        let mut graph = Self {
            graph: StableDiGraph::<HepNode, usize, usize>::default(),
            root: HepNodeId::default(),
        };
        let opt_expr = OptExpr::new_from_plan_ref(&root);
        graph.root = graph.add_opt_expr(opt_expr);
        graph
    }

    /// If input node is join, we use the edge weight to control the join chilren order.
    pub fn children_at(&self, id: HepNodeId) -> Vec<HepNodeId> {
        let mut children = self
            .graph
            .neighbors_directed(id, petgraph::Direction::Outgoing)
            .collect::<Vec<_>>();
        if children.len() > 1 {
            children.sort_by(|a, b| {
                let a_edge = self.graph.find_edge(id, *a).unwrap();
                let a_weight = self.graph.edge_weight(a_edge).unwrap();
                let b_edge = self.graph.find_edge(id, *b).unwrap();
                let b_weight = self.graph.edge_weight(b_edge).unwrap();
                a_weight.cmp(b_weight)
            })
        }
        children
    }

    #[allow(clippy::needless_collect)]
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

                // We should make sure the children order always be `left, right`. However, this
                // convention will break when a rule replaced the join child node with a node, it
                // will break the edge order in graph's edges vector. So this result in
                // `graph.neighbors_directed` method return vector order is incorrect and unstable.
                //
                // So we introduce edge weight to make sure the order. The edge weight more larger,
                // the target node is more to right. And also remember the edge weight should keep
                // same when replace_node.
                let children_ids = opt_expr
                    .children
                    .into_iter()
                    .map(|p| self.add_opt_expr(p))
                    .collect::<Vec<_>>();

                for (child_order, child_hep_id) in children_ids.into_iter().enumerate() {
                    self.graph.add_edge(new_node_id, child_hep_id, child_order);
                }

                new_node_id
            }
        }
    }

    /// Convert the graph to a plan tree, recursively process children and construct new plan.
    pub fn to_plan(&self) -> PlanRef {
        self.to_plan_start_from(self.root)
    }

    pub fn to_plan_start_from(&self, start: HepNodeId) -> PlanRef {
        let ids = self.children_at(start);

        // recursively process children's children
        let children = ids
            .iter()
            .map(|&id| self.to_plan_start_from(id))
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

    /// Use bfs to traverse the graph and return node ids. If the node is a join, the children order
    /// is unstable. Maybe `left, right` or `right, left`.
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
        // hold the old node's parents before add new node
        let parent_ids = self
            .graph
            .neighbors_directed(old_node_id, petgraph::Direction::Incoming)
            .collect::<Vec<_>>();

        // keep original edge weight to fix join child ordering
        let parent_ids_with_edge_wights = parent_ids
            .iter()
            .map(|id| {
                let edge = self.graph.find_edge(*id, old_node_id).unwrap();
                let weight = self.graph.edge_weight(edge).unwrap();
                (*id, *weight)
            })
            .collect::<Vec<_>>();

        // add new node and rectify edges with existing children nodes
        let new_node_id = self.add_opt_expr(new_opt_expr);

        // change replaced node's parents point to new child
        for (parent_id, weight) in parent_ids_with_edge_wights {
            self.graph.add_edge(parent_id, new_node_id, weight);
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
    use pretty_assertions::assert_eq;
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::{BoundBinaryOp, BoundExpr, BoundOrderBy, JoinCondition, JoinType};
    use crate::optimizer::{
        Dummy, LogicalJoin, LogicalLimit, LogicalOrder, LogicalProject, LogicalTableScan,
        PlanNodeType, PlanTreeNode,
    };
    use crate::util::pretty_plan_tree_string;

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
        //      2 <-----------left: Join {
        //          3 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      5 <-----------right: t3
        //                }
        //            }
        let node_ids = graph.children_at(1.into());
        assert_eq!(node_ids, vec![2.into(), 5.into()]);

        let node_ids = graph.children_at(2.into());
        assert_eq!(node_ids, vec![3.into(), 4.into()]);

        let node_ids = graph.children_at(0.into());
        assert_eq!(node_ids, vec![1.into()]);
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
        //      2 <-----------left: Join {
        //          3 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      5 <-----------right: t3
        //                }
        //            }

        // down-top returned join children order is [left, right] if join children node not changed
        // by rule.
        let bottom_up_ids = graph
            .nodes_iter(HepMatchOrder::BottomUp)
            .collect::<Vec<_>>();
        assert_eq!(
            bottom_up_ids,
            vec![
                HepNodeId::new(3),
                HepNodeId::new(4),
                HepNodeId::new(2),
                HepNodeId::new(5),
                HepNodeId::new(1),
                HepNodeId::new(0),
            ]
        );

        // top-down returned join children order is [right, left] if join children node not changed
        // by rule.
        let top_down_ids = graph.nodes_iter(HepMatchOrder::TopDown).collect::<Vec<_>>();
        assert_eq!(
            top_down_ids,
            vec![
                HepNodeId::new(0),
                HepNodeId::new(1),
                HepNodeId::new(5),
                HepNodeId::new(2),
                HepNodeId::new(4),
                HepNodeId::new(3),
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

        let ids = graph.children_at(2.into());
        assert_eq!(ids, vec![3.into(), 4.into()]);
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
        //      2 <-----------left: Join {
        //          3 <-----------t1,
        //          4 <-----------t2
        //                    },
        //      5 <-----------right: t3
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
        //          2 <-----------left: Join {
        //              3 <-----------t1,
        //              4 <-----------t2
        //                        },
        //          5 <-----------right: t3
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

    #[test]
    fn test_graph_replace_join_child_node_and_keep_left_right_order() {
        let join = LogicalJoin::new(
            Arc::new(build_logical_table_scan("t1")),
            Arc::new(build_logical_table_scan("t2")),
            JoinType::Right,
            build_join_condition_eq("t1", "c1", "t2", "c1"),
        );
        let limit = LogicalLimit::new(Some(BoundExpr::Constant(1.into())), None, Arc::new(join));
        let project =
            LogicalProject::new(vec![build_bound_column_ref("t1", "c1")], Arc::new(limit));

        // select t1.c1 from t1 right join t2 on t1.c1=t2.c1 limit 1
        // graph:
        // 0 <--------Project {
        //     1 <--------Limit {
        //       2 <----------Join {
        //          3 <-----------left:  t1
        //          4 <-----------right: t2
        //                    }
        //                }
        //            }
        let mut graph = HepGraph::new(Arc::new(project));

        // we assume that have a rule will match join right child, and push down extra limit into
        // right side.

        let new_right_child = OptExpr {
            root: OptExprNode::PlanRef(Arc::new(LogicalLimit::new(
                Some(BoundExpr::Constant(1.into())),
                None,
                Dummy::new_ref(),
            ))),
            children: vec![OptExpr {
                root: OptExprNode::PlanRef(Arc::new(build_logical_table_scan("t2"))),
                children: vec![],
            }],
        };

        // after replace, we should keep t2 still be right child of join.
        graph.replace_node(4.into(), new_right_child);

        let expect = r"
LogicalProject: exprs [t1.c1:Nullable(Int32)]
  LogicalLimit: limit Some(1), offset None
    LogicalJoin: type Right, cond On { on: [(t1.c1:Nullable(Int32), t2.c1:Nullable(Int32))], filter: None }
      LogicalTableScan: table: #t1, columns: [c1, c2]
      LogicalLimit: limit Some(1), offset None
        LogicalTableScan: table: #t2, columns: [c1, c2]";
        let actual = pretty_plan_tree_string(&*graph.to_plan());
        assert_eq!(
            expect.trim_start(),
            actual.trim_end(),
            "actual plan:\n{}",
            actual
        );
    }
}
