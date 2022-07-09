use std::fmt;

use crate::binder::BoundExpr;

use super::PlanRef;

pub struct LogicalFilter {
    /// filtered expression on input PlanRef
    expr: BoundExpr,
    /// the child PlanRef to be projected
    _input: PlanRef,
}

impl fmt::Display for LogicalFilter {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "LogicalFilter: expr {:?}", self.expr)
    }
}
