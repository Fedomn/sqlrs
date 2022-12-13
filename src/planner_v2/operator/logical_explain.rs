use derive_new::new;

use super::LogicalOperatorBase;

#[derive(new, Debug, Clone)]
pub struct LogicalExplain {
    pub(crate) base: LogicalOperatorBase,
    #[allow(dead_code)]
    pub(crate) explain_type: ExplainType,
    /// un-optimized logical plan explain string
    pub(crate) logical_plan: String,
}

#[derive(Debug, Clone)]
pub enum ExplainType {
    STANDARD,
    ANALYZE,
}
