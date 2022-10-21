use std::fmt;
use std::sync::Arc;

use super::{LogicalProject, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::{ColumnCatalog, TableId};

#[derive(Debug, Clone)]
pub struct PhysicalProject {
    logical: LogicalProject,
}

impl PhysicalProject {
    pub fn new(logical: LogicalProject) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalProject {
        &self.logical
    }
}

impl PlanNode for PhysicalProject {
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

impl PlanTreeNode for PhysicalProject {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_project().unwrap().clone()))
    }
}

impl fmt::Display for PhysicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalProject: exprs {:?}", self.logical().exprs())
    }
}

impl PartialEq for PhysicalProject {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
