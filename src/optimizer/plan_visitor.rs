use paste::paste;

use super::plan_node::*;
use crate::for_all_plan_nodes;

macro_rules! def_rewriter {
    ($($node_name:ident),*) => {
        pub trait PlanVisitor<R> {
            paste! {
                fn visit(&mut self, plan: PlanRef) -> Option<R> {
                    match plan.node_type() {
                        $(
                            PlanNodeType::$node_name => self.[<visit_$node_name:snake>](plan.downcast_ref::<$node_name>().unwrap()),
                        )*
                    }
                }

                $(
                    fn [<visit_$node_name:snake>](&mut self, _plan: &$node_name) -> Option<R> {
                        unimplemented!("The {} is not implemented visitor yet", stringify!($node_name))
                    }
                )*
            }
        }
    };
}

for_all_plan_nodes! { def_rewriter }
