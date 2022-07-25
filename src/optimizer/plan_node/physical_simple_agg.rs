use std::fmt;

use super::{LogicalAgg, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalSimpleAgg {
    logical: LogicalAgg,
}

impl PhysicalSimpleAgg {
    pub fn new(logical: LogicalAgg) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalAgg {
        &self.logical
    }
}

impl PlanNode for PhysicalSimpleAgg {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalSimpleAgg {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalSimpleAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalSimpleAgg: agg_funcs {:?} group_by {:?}",
            self.logical().agg_funcs(),
            self.logical().group_by(),
        )
    }
}
