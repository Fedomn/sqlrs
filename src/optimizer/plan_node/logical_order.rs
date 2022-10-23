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
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.order_by
            .iter()
            .flat_map(|e| e.expr.get_referenced_column_catalog())
            .collect::<Vec<_>>()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.children()[0].output_columns()
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

impl PartialEq for LogicalOrder {
    fn eq(&self, other: &Self) -> bool {
        self.order_by == other.order_by && self.input == other.input()
    }
}
