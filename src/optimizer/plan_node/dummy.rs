use std::fmt;

use super::PlanNode;

#[derive(Debug, Clone)]
pub struct Dummy {}

impl PlanNode for Dummy {}

impl fmt::Display for Dummy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Dummy:")
    }
}
