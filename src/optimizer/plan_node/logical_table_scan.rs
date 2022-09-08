use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::{ColumnCatalog, TableId};

#[derive(Debug, Clone)]
pub struct LogicalTableScan {
    table_id: TableId,
    columns: Vec<ColumnCatalog>,
}

impl LogicalTableScan {
    pub fn new(table_id: TableId, columns: Vec<ColumnCatalog>) -> Self {
        Self { table_id, columns }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id.clone()
    }

    pub fn column_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.column_id.clone()).collect()
    }

    pub fn columns(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
    }
}

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
    }
}

impl PlanTreeNode for LogicalTableScan {
    fn children(&self) -> Vec<PlanRef> {
        vec![]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 0);
        Arc::new(self.clone())
    }
}

impl fmt::Display for LogicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalTableScan: table: #{}, columns: [{}]",
            self.table_id(),
            self.column_ids().join(", ")
        )
    }
}

impl PartialEq for LogicalTableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id && self.columns == other.columns
    }
}
