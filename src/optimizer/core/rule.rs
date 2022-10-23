use enum_dispatch::enum_dispatch;

use super::{OptExpr, Pattern};

/// A rule is to transform logically equivalent expression. There are two kinds of rules:
///
/// - Transformation Rule: Logical to Logical
/// - Implementation Rule: Logical to Physical
#[enum_dispatch]
pub trait Rule {
    /// The pattern to determine whether the rule can be applied.
    fn pattern(&self) -> &Pattern;

    /// Apply the rule and write the transformation result to `Substitute`.
    /// The pattern tree determines the opt_expr tree internal nodes type.
    fn apply(&self, opt_expr: OptExpr, result: &mut Substitute);
}

/// Define the transformed plans
#[derive(Default)]
pub struct Substitute {
    pub opt_exprs: Vec<OptExpr>,
}
