use std::fmt;
use std::sync::Arc;

use super::{PlanNode, PlanRef, PlanTreeNode};
use crate::binder::BoundExpr;
use crate::catalog::{ColumnCatalog, TableId};

#[derive(Debug, Clone)]
pub struct LogicalAgg {
    agg_funcs: Vec<BoundExpr>,
    group_by: Vec<BoundExpr>,
    input: PlanRef,
}

impl LogicalAgg {
    pub fn new(agg_funcs: Vec<BoundExpr>, group_by: Vec<BoundExpr>, input: PlanRef) -> Self {
        Self {
            agg_funcs,
            group_by,
            input,
        }
    }

    pub fn agg_funcs(&self) -> Vec<BoundExpr> {
        self.agg_funcs.clone()
    }

    pub fn group_by(&self) -> Vec<BoundExpr> {
        self.group_by.clone()
    }

    pub fn input(&self) -> PlanRef {
        self.input.clone()
    }
}

impl PlanNode for LogicalAgg {
    fn referenced_columns(&self) -> Vec<ColumnCatalog> {
        self.group_by
            .iter()
            .chain(self.agg_funcs.iter())
            .flat_map(|e| e.get_referenced_column_catalog())
            .collect::<Vec<_>>()
    }

    fn output_new_columns(&self, base_table_id: String) -> Vec<ColumnCatalog> {
        self.group_by
            .iter()
            .chain(self.agg_funcs.iter())
            .map(|e| e.output_column_catalog_for_alias_table(base_table_id.clone()))
            .collect::<Vec<_>>()
    }

    fn get_based_table_id(&self) -> TableId {
        self.children()[0].get_based_table_id()
    }
}

impl PlanTreeNode for LogicalAgg {
    fn children(&self) -> Vec<PlanRef> {
        vec![self.input.clone()]
    }

    fn clone_with_children(&self, children: Vec<PlanRef>) -> PlanRef {
        assert_eq!(children.len(), 1);
        Arc::new(Self::new(
            self.agg_funcs.clone(),
            self.group_by.clone(),
            children[0].clone(),
        ))
    }
}

impl fmt::Display for LogicalAgg {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(
            f,
            "LogicalAgg: agg_funcs {:?} group_by {:?}",
            self.agg_funcs(),
            self.group_by(),
        )
    }
}

impl PartialEq for LogicalAgg {
    fn eq(&self, other: &Self) -> bool {
        self.agg_funcs == other.agg_funcs
            && self.group_by == other.group_by
            && self.input == other.input()
    }
}
