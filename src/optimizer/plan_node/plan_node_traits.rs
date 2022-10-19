use crate::for_all_plan_nodes;
use crate::optimizer::plan_node::*;

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}

/// impl [`PlanNodeType`] fn for each node.
macro_rules! enum_plan_node_type {
    ($($node_name:ident),*) => {
        /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
        #[derive(Debug, Clone, PartialEq, Eq)]
        pub enum PlanNodeType {
            $($node_name),*
        }

        $(impl WithPlanNodeType for $node_name {
            fn node_type(&self) -> PlanNodeType {
                PlanNodeType::$node_name
            }
        })*
    };
}

for_all_plan_nodes! { enum_plan_node_type }

/// The trait is used by optimizer for rewriting plan nodes.
/// every plan node should implement this trait.
pub trait PlanTreeNode {
    /// Get the child plan nodes.
    fn children(&self) -> Vec<PlanRef>;

    /// Clone the node with new children for rewriting plan node.
    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef;
}

impl PartialEq for dyn PlanNode {
    fn eq(&self, other: &Self) -> bool {
        if self.type_id() != other.type_id() {
            false
        } else {
            match self.node_type() {
                PlanNodeType::Dummy => self.as_dummy() == other.as_dummy(),
                PlanNodeType::LogicalTableScan => {
                    self.as_logical_table_scan() == other.as_logical_table_scan()
                }
                PlanNodeType::LogicalProject => {
                    self.as_logical_project() == other.as_logical_project()
                }
                PlanNodeType::LogicalFilter => {
                    self.as_logical_filter() == other.as_logical_filter()
                }
                PlanNodeType::LogicalAgg => self.as_logical_agg() == other.as_logical_agg(),
                PlanNodeType::LogicalLimit => self.as_logical_limit() == other.as_logical_limit(),
                PlanNodeType::LogicalOrder => self.as_logical_order() == other.as_logical_order(),
                PlanNodeType::LogicalJoin => self.as_logical_join() == other.as_logical_join(),
                PlanNodeType::PhysicalTableScan => {
                    self.as_physical_table_scan() == other.as_physical_table_scan()
                }
                PlanNodeType::PhysicalProject => {
                    self.as_physical_project() == other.as_physical_project()
                }
                PlanNodeType::PhysicalFilter => {
                    self.as_physical_filter() == other.as_physical_filter()
                }
                PlanNodeType::PhysicalSimpleAgg => {
                    self.as_physical_simple_agg() == other.as_physical_simple_agg()
                }
                PlanNodeType::PhysicalHashAgg => {
                    self.as_physical_hash_agg() == other.as_physical_hash_agg()
                }
                PlanNodeType::PhysicalLimit => {
                    self.as_physical_limit() == other.as_physical_limit()
                }
                PlanNodeType::PhysicalOrder => {
                    self.as_physical_order() == other.as_physical_order()
                }
                PlanNodeType::PhysicalHashJoin => {
                    self.as_physical_hash_join() == other.as_physical_hash_join()
                }
                PlanNodeType::PhysicalCrossJoin => {
                    self.as_physical_cross_join() == other.as_physical_cross_join()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::DataType;
    use sqlparser::ast::BinaryOperator;

    use crate::binder::test_util::*;
    use crate::binder::{BoundBinaryOp, BoundExpr};
    use crate::optimizer::{LogicalFilter, LogicalProject, LogicalTableScan, PlanRef};

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

    fn build_plan_tree(table_id: &str) -> PlanRef {
        let plan = build_logical_table_scan(table_id);
        let filter_plan = build_logical_filter(Arc::new(plan));
        let project_plan = build_logical_project(Arc::new(filter_plan));
        Arc::new(project_plan)
    }

    #[test]
    fn test_equals_two_dyn_plan_node() {
        let plan1 = build_plan_tree("t1");
        let plan2 = build_plan_tree("t1");
        assert!(plan1 == plan2);

        let plan3 = Arc::new(build_logical_filter(Arc::new(build_logical_table_scan(
            "t3",
        ))));
        assert!(plan1 != plan3);
    }
}
