use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct LogicalFilter {
    /// filtered expression on input PlanRef
    expr: BoundExpr,
    /// the child PlanRef to be projected
    input: PlanRef,
}

impl LogicalFilter {
    pub fn new(expr: BoundExpr, input: PlanRef) -> Self {
        Self { expr, input }
    }

    pub fn expr(&self) -> BoundExpr {
        self.expr.clone()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl PlanNode for LogicalFilter {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.expr.get_referenced_column_catalog()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.children()[0].output_columns()
    }
}

impl PlanTreeNode for LogicalFilter {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(self.expr.clone(), children[0].clone()))
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr {:?}", self.expr)
    }
}

impl PartialEq for LogicalFilter {
    fn eq(&self, other: &Self) -> bool {
        self.expr == other.expr && self.input == other.input()
    }
}
