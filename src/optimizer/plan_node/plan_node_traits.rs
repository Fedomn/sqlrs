use crate::optimizer::plan_node::*;

use crate::for_all_plan_nodes;

pub trait WithPlanNodeType {
    fn node_type(&self) -> PlanNodeType;
}

/// impl [`PlanNodeType`] fn for each node.
macro_rules! enum_plan_node_type {
    ($($node_name:ident),*) => {
        /// each enum value represent a PlanNode struct type, help us to dispatch and downcast
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
