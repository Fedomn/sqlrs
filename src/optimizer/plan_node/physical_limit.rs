use core::fmt;

use super::{LogicalLimit, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalLimit {
    logical: LogicalLimit,
}

impl PhysicalLimit {
    pub fn new(logical: LogicalLimit) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalLimit {
        &self.logical
    }
}

impl PlanNode for PhysicalLimit {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical().schema()
    }
}

impl PlanTreeNode for PhysicalLimit {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        self.logical().clone_with_children(children)
    }
}

impl fmt::Display for PhysicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalLimit: limit {:?}, offset {:?}",
            self.logical().limit(),
            self.logical().offset(),
        )
    }
}
