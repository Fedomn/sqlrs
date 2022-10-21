use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::catalog::ColumnCatalog;

#[derive(Debug, Clone)]
pub struct Dummy {}

impl Dummy {
    pub fn new_ref() -> PlanRef {
        Arc::new(Self {})
    }

    pub fn new_refs(cnt: usize) -> Vec<PlanRef> {
        (0..cnt).into_iter().map(|_| Dummy::new_ref()).collect()
    }
}

impl PlanNode for Dummy {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        vec![]
    }

    fn output_columns(&self) -> Vec<ColumnCatalog> {
        vec![]
    }

    fn output_new_columns(&self, _base_table_id: String) -> Vec<ColumnCatalog> {
        vec![]
    }

    fn get_based_table_id(&self) -> crate::catalog::TableId {
        "Dummy".to_string()
    }
}

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

impl PartialEq for Dummy {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
