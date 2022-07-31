use std::fmt;

use super::{LogicalAgg, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalHashAgg {
    logical: LogicalAgg,
}

impl PhysicalHashAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalAgg {
        &self.logical
    }
}

impl PlanNode for PhysicalHashAgg {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalHashAgg {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalHashAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalHashAgg: agg_funcs {:?} group_by {:?}",
            self.logical().agg_funcs(),
            self.logical().group_by(),
        )
    }
}
