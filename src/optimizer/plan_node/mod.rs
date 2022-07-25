mod dummy;
mod logical_agg;
mod logical_filter;
mod logical_project;
mod logical_table_scan;
mod physical_filter;
mod physical_project;
mod physical_simple_agg;
mod physical_table_scan;
mod plan_node_traits;

use std::fmt::Debug;
use std::sync::Arc;

use downcast_rs::{impl_downcast, Downcast};
pub use dummy::*;
pub use logical_agg::*;
pub use logical_filter::*;
pub use logical_project::*;
pub use logical_table_scan::*;
use paste::paste;
pub use physical_filter::*;
pub use physical_project::*;
pub use physical_simple_agg::*;
pub use physical_table_scan::*;
pub use plan_node_traits::*;

use crate::catalog::ColumnCatalog;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as
/// `dyn PlanNode`. Meanwhile, we split the trait into lots of sub-traits so that we can easily use
/// macro to impl them.
pub trait PlanNode: WithPlanNodeType + PlanTreeNode + Debug + Downcast + Send + Sync {
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![]
    }
}
impl_downcast!(PlanNode);

/// The type of reference to a plan node.
pub type PlanRef = Arc<dyn PlanNode>;

/// The core idea of `for_all_plan_nodes` is to generate boilerplate code for all plan nodes,
/// which means passing the name of a macro into another macro.
///
/// We use this pattern to impl a trait for all plan nodes.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:ident) => {
        $macro! {
            Dummy,
            LogicalTableScan,
            LogicalProject,
            LogicalFilter,
            LogicalAgg,
            PhysicalTableScan,
            PhysicalProject,
            PhysicalFilter,
            PhysicalSimpleAgg
        }
    };
}

macro_rules! impl_downcast_utility {
    ($($node_name:ident),*) => {
        impl dyn PlanNode {
            $(
                paste! {
                    #[allow(dead_code, clippy::result_unit_err)]
                    pub fn [<as_ $node_name:snake>] (&self) -> std::result::Result<&$node_name, ()> {
                        self.downcast_ref::<$node_name>().ok_or_else(|| ())
                    }
                }
            )*
        }
    }
}
for_all_plan_nodes! { impl_downcast_utility }
