use derive_new::new;

use super::LogicalOperatorBase;

/// LogicalDummyScan represents a dummy scan returning nothing.
#[derive(new, Debug)]
pub struct LogicalDummyScan {
    #[new(default)]
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
}
