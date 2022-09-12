use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct LogicalLimit {
    limit: Option<BoundExpr>,
    offset: Option<BoundExpr>,
    input: PlanRef,
}

impl LogicalLimit {
    pub fn new(limit: Option<BoundExpr>, offset: Option<BoundExpr>, input: PlanRef) -> Self {
        Self {
            limit,
            offset,
            input,
        }
    }

    pub fn limit(&self) -> Option<BoundExpr> {
        self.limit.clone()
    }

    pub fn offset(&self) -> Option<BoundExpr> {
        self.offset.clone()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl PlanNode for LogicalLimit {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        vec![]
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.children()[0].output_columns()
    }
}

impl PlanTreeNode for LogicalLimit {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(self.limit(), self.offset(), children[0].clone()))
    }
}

impl fmt::Display for LogicalLimit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalLimit: limit {:?}, offset {:?}",
            self.limit, self.offset
        )
    }
}

impl PartialEq for LogicalLimit {
    fn eq(&self, other: &Self) -> bool {
        self.limit == other.limit && self.offset == other.offset && self.input == other.input()
    }
}
