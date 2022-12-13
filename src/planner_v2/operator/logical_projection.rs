use derive_new::new;

use super::LogicalOperatorBase;

#[derive(new, Debug, Clone)]
pub struct LogicalProjection {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
}
