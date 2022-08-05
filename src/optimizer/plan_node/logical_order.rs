use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundOrderBy;
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct LogicalOrder {
    order_by: Vec<BoundOrderBy>,
    input: PlanRef,
}

impl LogicalOrder {
    pub fn new(order_by: Vec<BoundOrderBy>, input: PlanRef) -> Self {
        Self { order_by, input }
    }

    pub fn order_by(&self) -> Vec<BoundOrderBy> {
        self.order_by.clone()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl PlanNode for LogicalOrder {
    fn schema(&self) -> Vec<ColumnCatalog> {
        self.input.schema()
    }
}

impl PlanTreeNode for LogicalOrder {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(self.order_by(), children[0].clone()))
    }
}

impl fmt::Display for LogicalOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalOrder: order {:?}", self.order_by)
    }
}
