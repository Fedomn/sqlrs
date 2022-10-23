use core::fmt;
use std::sync::Arc;

use super::{LogicalJoin, PlanNode, PlanRef, PlanTreeNode};
use crate::binder::{JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct PhysicalHashJoin {
    logical: LogicalJoin,
}

impl PhysicalHashJoin {
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

    pub fn join_condition(&self) -> JoinCondition {
        self.logical.join_condition()
    }

    pub fn logical(&self) -> &LogicalJoin {
        &self.logical
    }

    pub fn join_output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.join_output_columns()
    }
}

impl PlanNode for PhysicalHashJoin {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.logical.referenced_columns()
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        self.logical().output_columns()
    }
}

impl PlanTreeNode for PhysicalHashJoin {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.left(), self.right()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        let p = self.logical().clone_with_children(children);
        Arc::new(Self::new(p.as_logical_join().unwrap().clone()))
    }
}

impl fmt::Display for PhysicalHashJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "PhysicalHashJoin: type {:?}, cond {:?}",
            self.join_type(),
            self.join_condition()
        )
    }
}

impl PartialEq for PhysicalHashJoin {
    fn eq(&self, other: &Self) -> bool {
        self.join_type() == other.join_type()
            && self.join_condition() == other.join_condition()
            && self.left() == other.left()
            && self.right() == other.right()
    }
}
