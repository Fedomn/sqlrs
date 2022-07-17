use itertools::Itertools;
use paste::paste;

use super::plan_node::*;
use crate::for_all_plan_nodes;

macro_rules! def_rewriter {
    ($($node_name:ident),*) => {
        pub trait PlanRewriter {
            paste! {
                fn rewrite(&mut self, plan: PlanRef) -> PlanRef {
                    match plan.node_type() {
                        $(
                            PlanNodeType::$node_name => self.[<rewrite_$node_name:snake>](plan.downcast_ref::<$node_name>().unwrap()),
                        )*
                    }
                }

                $(
                    fn [<rewrite_$node_name:snake>](&mut self, plan: &$node_name) -> PlanRef {
                        let new_children = plan
                            .children()
                            .into_iter()
                            .map(|child| self.rewrite(child.clone()))
                            .collect_vec();
                        plan.clone_with_children(new_children)
                    }
                )*
            }
        }
    };
}

for_all_plan_nodes! { def_rewriter }
