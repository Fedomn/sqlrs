use std::fmt;
use std::sync::Arc;

use super::{LogicalTableScan, PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalTableScan {
    logical: LogicalTableScan,
}

impl PhysicalTableScan {
    pub fn new(logical: LogicalTableScan) -> Self {
        Self { logical }
    }

    pub fn logical(&self) -> &LogicalTableScan {
        &self.logical
    }
}

impl PlanNode for PhysicalTableScan {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.logical.schema()
    }
}

impl PlanTreeNode for PhysicalTableScan {
    fn children(&self) -> Vec<PlanRef> {
        vec![]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 0);
        Arc::new(self.clone())
    }
}

impl fmt::Display for PhysicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalTableScan: table: #{}, columns: [{}]",
            self.logical().table_id(),
            self.logical().column_ids().join(", ")
        )
    }
}
