use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::catalog::{ColumnCatalog, TableId};

#[derive(Debug, Clone)]
pub struct LogicalProject {
    /// evaluated projection expressions on input PlanRef
    exprs: Vec<BoundExpr>,
    /// the child PlanRef to be projected
    input: PlanRef,
}

impl LogicalProject {
    pub fn new(exprs: Vec<BoundExpr>, input: PlanRef) -> Self {
        Self { exprs, input }
    }

    pub fn exprs(&self) -> Vec<BoundExpr> {
        self.exprs.clone()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl PlanNode for LogicalProject {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.output_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.exprs
            .iter()
            .flat_map(|e| e.get_referenced_column_catalog())
            .collect::<Vec<_>>()
    }

    fn output_new_columns(&self, base_table_id: String) -> Vec<ColumnCatalog> {
        self.exprs
            .iter()
            .map(|e| e.output_column_catalog_for_alias_table(base_table_id.clone()))
            .collect::<Vec<_>>()
    }

    fn get_based_table_id(&self) -> TableId {
        self.children()[0].get_based_table_id()
    }
}

impl PlanTreeNode for LogicalProject {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(self.exprs.clone(), children[0].clone()))
    }
}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProject: exprs {:?}", self.exprs)
    }
}

impl PartialEq for LogicalProject {
    fn eq(&self, other: &Self) -> bool {
        self.exprs == other.exprs && self.input == other.input()
    }
}
