use std::fmt;
use std::sync::Arc;

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
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
    }
}

impl PlanTreeNode for PhysicalHashAgg {
    fn children(&self) -> Vec<PlanRef> {
        self.logical().children()
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_agg().unwrap().clone()))
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

impl PartialEq for PhysicalHashAgg {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
