use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};

#[derive(Debug, Clone)]
pub struct Dummy {}

impl PlanNode for Dummy {}

impl PlanTreeNode for Dummy {
    fn children(&self) -> Vec<PlanRef> {
        vec![]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 0);
        Arc::new(self.clone())
    }
}

impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
