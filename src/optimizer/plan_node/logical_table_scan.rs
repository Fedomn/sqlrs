use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::{ColumnCatalog, TableId};

#[derive(Debug, Clone)]
pub struct LogicalTableScan {
    table_id: TableId,
    table_alias: Option<String>,
    columns: Vec<ColumnCatalog>,
    /// optional bounds of the reader, of the form (offset, limit).
    bounds: Option<(usize, usize)>,
    /// the projections is column indices.
    projections: Option<Vec<usize>>,
}

impl LogicalTableScan {
    pub fn new(
        table_id: TableId,
        table_alias: Option<String>,
        columns: Vec<ColumnCatalog>,
        bounds: Option<(usize, usize)>,
        projections: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table_id,
            table_alias,
            columns,
            bounds,
            projections,
        }
    }

    pub fn table_id(&self) -> TableId {
        self.table_id.clone()
    }

    pub fn table_alias(&self) -> Option<String> {
        self.table_alias.clone()
    }

    pub fn column_ids(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.column_id.clone()).collect()
    }

    pub fn columns(&self) -> Vec<ColumnCatalog> {
        self.columns.clone()
    }

    pub fn bounds(&self) -> Option<(usize, usize)> {
        self.bounds
    }

    pub fn projections(&self) -> Option<Vec<usize>> {
        self.projections.clone()
    }
}

impl PlanNode for LogicalTableScan {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        if let Some(alias) = self.table_alias() {
            self.columns()
                .iter()
                .map(|c| c.clone_with_table_id(alias.clone()))
                .collect()
        } else {
            self.columns()
        }
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
        let bounds_str = self
            .bounds()
            .map(|b| format!(", bounds: (offset:{},limit:{})", b.0, b.1))
            .unwrap_or_else(|| "".into());
        let alias = self
            .table_alias()
            .map(|alias| format!(" as {}", alias))
            .unwrap_or_else(|| "".into());
        writeln!(
            f,
            "LogicalTableScan: table: #{}{}, columns: [{}]{}",
            self.table_id(),
            alias,
            self.column_ids().join(", "),
            bounds_str,
        )
    }
}

impl PartialEq for LogicalTableScan {
    fn eq(&self, other: &Self) -> bool {
        self.table_id == other.table_id && self.columns == other.columns
    }
}
