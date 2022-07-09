use std::fmt;

use crate::binder::BoundExpr;

use super::PlanRef;

pub struct LogicalProject {
    /// evaluated projection expressions on input PlanRef
    pub exprs: Vec<BoundExpr>,
    /// the child PlanRef to be projected
    pub input: PlanRef,
}

impl fmt::Display for LogicalProject {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalProject: exprs {:?}", self.exprs)
    }
}
