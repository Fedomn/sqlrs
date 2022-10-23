use core::fmt;
use std::sync::Arc;

use super::{LogicalJoin, PlanNode, PlanRef, PlanTreeNode};
use crate::binder::JoinType;
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalCrossJoin {
    logical: LogicalJoin,
}

impl PhysicalCrossJoin {
    pub fn new(logical: LogicalJoin) -> Self {
        Self { logical }
    }

    pub fn left(&self) -> PlanRef {
        self.logical.left()
    }

    pub fn right(&self) -> PlanRef {
        self.logical.right()
    }

    pub fn join_type(&self) -> JoinType {
        self.logical.join_type()
    }

    pub fn logical(&self) -> &LogicalJoin {
        &self.logical
    }

    pub fn join_output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.join_output_columns()
    }
}

impl PlanNode for PhysicalCrossJoin {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
    }
}

impl PlanTreeNode for PhysicalCrossJoin {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.left(), self.right()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_join().unwrap().clone()))
    }
}

impl fmt::Display for PhysicalCrossJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "PhysicalCrossJoin: type {:?}", self.join_type(),)
    }
}

impl PartialEq for PhysicalCrossJoin {
    fn eq(&self, other: &Self) -> bool {
        self.join_type() == other.join_type()
            && self.left() == other.left()
            && self.right() == other.right()
    }
}
