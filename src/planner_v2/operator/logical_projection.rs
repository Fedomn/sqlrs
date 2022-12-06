use derive_new::new;

use super::LogicalOperatorBase;

#[derive(new, Debug)]
pub struct LogicalProjection {
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
}
