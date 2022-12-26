use derive_new::new;

use super::LogicalOperatorBase;

/// LogicalDummyScan represents a dummy scan returning  a single row.
#[derive(new, Debug, Clone)]
pub struct LogicalDummyScan {
    #[new(default)]
    pub(crate) base: LogicalOperatorBase,
    pub(crate) table_idx: usize,
}
