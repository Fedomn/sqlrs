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
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
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
        let bounds_str = self
            .logical()
            .bounds()
            .map(|b| format!(", bounds: (offset:{},limit:{})", b.0, b.1))
            .unwrap_or_else(|| "".into());
        let alias = self
            .logical()
            .table_alias()
            .map(|alias| format!(" as {}", alias))
            .unwrap_or_else(|| "".into());
        writeln!(
            f,
            "PhysicalTableScan: table: #{}{}, columns: [{}]{}",
            self.logical().table_id(),
            alias,
            self.logical().column_ids().join(", "),
            bounds_str,
        )
    }
}

impl PartialEq for PhysicalTableScan {
    fn eq(&self, other: &Self) -> bool {
        self.logical == other.logical
    }
}
