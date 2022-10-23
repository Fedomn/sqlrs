use core::fmt;
use std::sync::Arc;

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
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
    }
}

impl PlanTreeNode for PhysicalLimit {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_limit().unwrap().clone()))
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

impl PartialEq for PhysicalLimit {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
