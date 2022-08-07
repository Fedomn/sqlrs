use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::{JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct LogicalJoin {
    left: PlanRef,
    right: PlanRef,
    join_type: JoinType,
    join_condition: JoinCondition,
}

impl LogicalJoin {
    pub fn new(
        left: PlanRef,
        right: PlanRef,
        join_type: JoinType,
        join_condition: JoinCondition,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            join_condition,
        }
    }

    pub fn left(&self) -> PlanRef {
        self.left.clone()
    }

    pub fn right(&self) -> PlanRef {
        self.right.clone()
    }

    pub fn join_type(&self) -> JoinType {
        self.join_type.clone()
    }

    pub fn join_condition(&self) -> JoinCondition {
        self.join_condition.clone()
    }
}

impl PlanNode for LogicalJoin {
    fn schema(&self) -> Vec<ColumnCatalog> {
        vec![self.left.schema(), self.right.schema()].concat()
    }
}

impl PlanTreeNode for LogicalJoin {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 2);
        Arc::new(Self::new(
            children[0].clone(),
            children[1].clone(),
            self.join_type.clone(),
            self.join_condition.clone(),
        ))
    }
}

impl fmt::Display for LogicalJoin {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalJoin: op {:?}, cond {:?}",
            self.join_type, self.join_condition
        )
    }
}
