use std::fmt;

use crate::{binder::BoundExpr, catalog::ColumnCatalog};

use super::{PlanNode, PlanRef};

#[derive(Debug, Clone)]
pub struct LogicalProject {
    /// evaluated projection expressions on input PlanRef
    pub exprs: Vec<BoundExpr>,
    /// the child PlanRef to be projected
    pub input: PlanRef,
}

impl LogicalProject {
    pub fn new(exprs: Vec<BoundExpr>, input: PlanRef) -> Self {
        Self { exprs, input }
    }
}

impl PlanNode for LogicalProject {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
    }
}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProject: exprs {:?}", self.exprs)
    }
}
