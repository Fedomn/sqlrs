mod dummy;
mod logical_agg;
mod logical_filter;
mod logical_join;
mod logical_limit;
mod logical_order;
mod logical_project;
mod logical_table_scan;
mod physical_filter;
mod physical_hash_agg;
mod physical_hash_join;
mod physical_limit;
mod physical_order;
mod physical_project;
mod physical_simple_agg;
mod physical_table_scan;
mod plan_node_traits;

use std::fmt::{Debug, Display, Write};
use std::sync::Arc;

use downcast_rs::{impl_downcast, Downcast};
pub use dummy::*;
pub use logical_agg::*;
pub use logical_filter::*;
pub use logical_join::*;
pub use logical_limit::*;
pub use logical_order::*;
pub use logical_project::*;
pub use logical_table_scan::*;
use paste::paste;
pub use physical_filter::*;
pub use physical_hash_agg::*;
pub use physical_hash_join::*;
pub use physical_limit::*;
pub use physical_order::*;
pub use physical_project::*;
pub use physical_simple_agg::*;
pub use physical_table_scan::*;
pub use plan_node_traits::*;

use crate::catalog::ColumnCatalog;

/// The common trait over all plan nodes. Used by optimizer framework which will treat all node as
/// `dyn PlanNode`. Meanwhile, we split the trait into lots of sub-traits so that we can easily use
/// macro to impl them.
pub trait PlanNode:
    WithPlanNodeType + PlanTreeNode + Downcast + Debug + Display + Send + Sync
{
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![]
    }
}
impl_downcast!(PlanNode);

impl dyn PlanNode {
    pub fn explain(&self, level: usize, explain_result: &mut dyn Write) {
        let indented_self =
            format!("{}", self).replace("\n  ", &format!("\n{}", " ".repeat(level * 2 + 4)));
        write!(explain_result, "{}{}", " ".repeat(level * 2), indented_self).unwrap();
        for child in self.children() {
            child.explain(level + 1, explain_result);
        }
    }

    /// Clone a new plan that not contains children, currentlly used by optimizer graph.
    pub fn clone_with_dummy(&self) -> PlanRef {
        match self.node_type() {
            PlanNodeType::Dummy => self.clone_with_children(Dummy::new_refs(0)),
            PlanNodeType::LogicalTableScan => self.clone_with_children(Dummy::new_refs(0)),
            PlanNodeType::LogicalProject => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::LogicalFilter => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::LogicalAgg => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::LogicalLimit => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::LogicalOrder => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::LogicalJoin => self.clone_with_children(Dummy::new_refs(2)),
            PlanNodeType::PhysicalTableScan => self.clone_with_children(Dummy::new_refs(0)),
            PlanNodeType::PhysicalProject => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalFilter => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalSimpleAgg => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalHashAgg => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalLimit => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalOrder => self.clone_with_children(Dummy::new_refs(1)),
            PlanNodeType::PhysicalHashJoin => self.clone_with_children(Dummy::new_refs(2)),
        }
    }

    pub fn is_logical_plan(&self) -> bool {
        match self.node_type() {
            PlanNodeType::LogicalTableScan
            | PlanNodeType::LogicalProject
            | PlanNodeType::LogicalFilter
            | PlanNodeType::LogicalAgg
            | PlanNodeType::LogicalLimit
            | PlanNodeType::LogicalOrder
            | PlanNodeType::LogicalJoin => true,
            PlanNodeType::Dummy
            | PlanNodeType::PhysicalTableScan
            | PlanNodeType::PhysicalProject
            | PlanNodeType::PhysicalFilter
            | PlanNodeType::PhysicalSimpleAgg
            | PlanNodeType::PhysicalHashAgg
            | PlanNodeType::PhysicalLimit
            | PlanNodeType::PhysicalOrder
            | PlanNodeType::PhysicalHashJoin => false,
        }
    }

    pub fn contains_column_ref_expr(&self) -> bool {
        match self.node_type() {
            PlanNodeType::Dummy => false,
            PlanNodeType::LogicalTableScan => false,
            PlanNodeType::LogicalProject => self
                .as_logical_project()
                .unwrap()
                .exprs()
                .iter()
                .any(|e| e.contains_column_ref()),
            PlanNodeType::LogicalFilter => self
                .as_logical_filter()
                .unwrap()
                .expr()
                .contains_column_ref(),
            PlanNodeType::LogicalAgg => {
                let plan = self.as_logical_agg().unwrap();
                plan.group_by()
                    .iter()
                    .chain(plan.agg_funcs().iter())
                    .any(|e| e.contains_column_ref())
            }
            PlanNodeType::LogicalLimit => false,
            PlanNodeType::LogicalOrder => {
                let plan = self.as_logical_order().unwrap();
                plan.order_by().iter().any(|e| e.expr.contains_column_ref())
            }
            PlanNodeType::LogicalJoin => {
                let plan = self.as_logical_join().unwrap();
                plan.left().contains_column_ref_expr() || plan.right().contains_column_ref_expr()
            }
            PlanNodeType::PhysicalTableScan => false,
            PlanNodeType::PhysicalProject => false,
            PlanNodeType::PhysicalFilter => false,
            PlanNodeType::PhysicalSimpleAgg => false,
            PlanNodeType::PhysicalHashAgg => false,
            PlanNodeType::PhysicalLimit => false,
            PlanNodeType::PhysicalOrder => false,
            PlanNodeType::PhysicalHashJoin => false,
        }
    }
}

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
            LogicalLimit,
            LogicalOrder,
            LogicalJoin,
            PhysicalTableScan,
            PhysicalProject,
            PhysicalFilter,
            PhysicalSimpleAgg,
            PhysicalHashAgg,
            PhysicalLimit,
            PhysicalOrder,
            PhysicalHashJoin
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
