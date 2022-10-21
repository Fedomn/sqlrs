use core::fmt;
use std::sync::Arc;

use super::{LogicalFilter, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::{ColumnCatalog, TableId};

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
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self, base_table_id: String) -> Vec<ColumnCatalog> {
        self.logical().output_columns(base_table_id)
    }

    fn get_based_table_id(&self) -> TableId {
        self.logical().get_based_table_id()
    }
}

impl PlanTreeNode for PhysicalFilter {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_filter().unwrap().clone()))
    }
}

impl fmt::Display for PhysicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalFilter: expr {:?}", self.logical().expr())
    }
}

impl PartialEq for PhysicalFilter {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
