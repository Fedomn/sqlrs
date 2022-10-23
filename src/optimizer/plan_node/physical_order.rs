use core::fmt;
use std::sync::Arc;

use super::{LogicalOrder, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalOrder {
    logical: LogicalOrder,
}

impl PhysicalOrder {
    pub fn new(logical: LogicalOrder) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalOrder {
        &self.logical
    }
}

impl PlanNode for PhysicalOrder {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
    }
}

impl PlanTreeNode for PhysicalOrder {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_order().unwrap().clone()))
    }
}

impl fmt::Display for PhysicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalOrder: Order {:?}", self.logical().order_by())
    }
}

impl PartialEq for PhysicalOrder {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
