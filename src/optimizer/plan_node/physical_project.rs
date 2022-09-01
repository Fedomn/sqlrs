use std::fmt;
use std::sync::Arc;

use super::{LogicalProject, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

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
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical().schema()
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
