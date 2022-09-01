mod input_ref_rewrite;
mod physical_rewrite;
use std::fmt::Debug;

use enum_dispatch::enum_dispatch;
pub use input_ref_rewrite::*;
pub use physical_rewrite::*;
use strum_macros::AsRefStr;

use crate::optimizer::core::{OptExpr, Pattern, Rule, Substitute};

#[enum_dispatch(Rule)]
#[derive(Clone, AsRefStr)]
pub enum RuleImpl {
    InputRefRwriteRule,
    PhysicalRewriteRule,
}

impl Debug for RuleImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ref())
    }
}
