use derive_new::new;

use super::BoundExpressionBase;

/// A BoundReferenceExpression represents a physical index into a DataChunk
#[derive(new, Debug, Clone)]
pub struct BoundReferenceExpression {
    pub(crate) base: BoundExpressionBase,
    /// Index used to access data in the chunks
    pub(crate) index: usize,
}
