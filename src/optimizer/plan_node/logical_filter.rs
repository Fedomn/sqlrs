use super::{PlanNode, PlanRef};
use crate::{binder::BoundExpr, catalog::ColumnCatalog};
use std::fmt;

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
}

impl PlanNode for LogicalFilter {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
    }
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr {:?}", self.expr)
    }
}
