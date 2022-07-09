use std::fmt;

use itertools::Itertools;

use super::PlanNode;
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
}

impl PlanNode for LogicalTableScan {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
    }
}

impl fmt::Display for LogicalTableScan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalTableScan: table: #{}, columns: [{}]",
            self.table_id,
            self.columns.iter().map(|c| c.id.clone()).join(", ")
        )
    }
}
