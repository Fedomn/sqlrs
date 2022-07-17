use core::fmt;

use super::{LogicalFilter, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalFilter {
    logical: LogicalFilter,
}

impl PhysicalFilter {
    pub fn new(logical: LogicalFilter) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalFilter {
        &self.logical
    }
}

impl PlanNode for PhysicalFilter {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalFilter {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.logical().expr())
    }
}
