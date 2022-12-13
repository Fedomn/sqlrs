use arrow::record_batch::RecordBatch;
use derive_new::new;

use super::PhysicalOperatorBase;

/// The PhysicalColumnDataScan scans a Arrow RecordBatch
#[derive(new, Clone)]
pub struct PhysicalColumnDataScan {
    pub(crate) base: PhysicalOperatorBase,
    pub(crate) collection: Vec<RecordBatch>,
}
