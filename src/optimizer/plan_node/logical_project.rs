use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::catalog::ColumnCatalog;

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
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
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
