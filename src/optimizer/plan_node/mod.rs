mod dummy;
mod logical_table_scan;
mod plan_node_traits;

pub use dummy::*;
pub use logical_table_scan::*;
pub use plan_node_traits::*;

use crate::catalog::ColumnCatalog;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as `dyn PlanNode`.
/// Meanwhile, we split the trait into lots of sub-traits so that we can easily use macro to impl them.
pub trait PlanNode: WithPlanNodeType {
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![]
    }
}

/// The core idea of `for_all_plan_nodes` is to generate boilerplate code for all plan nodes,
/// which means passing the name of a macro into another macro.
///
/// We use this pattern to impl a trait for all plan nodes.
#[macro_export]
macro_rules! for_all_plan_nodes {
    ($macro:ident) => {
        $macro! {
            Dummy,
            LogicalTableScan
        }
    };
}
