use std::sync::Arc;

use super::Executor;
use crate::execution::PhysicalOperator;

/// Query is divided into pipelines, Pipelines are executed in parallel.
///
/// Pipeline represents an execution pipeline, it gets data from `source` and pass it to `operators`
/// for immediate computation, and then pass data to `sink` for final computation.
pub struct Pipeline {
    pub(crate) executor: Arc<Executor>,

    /// Whether or not the pipeline has been readied
    pub(crate) ready: bool,
    /// The source of this pipeline
    pub(crate) source: Option<PhysicalOperator>,
    /// The chain of intermediate operators
    pub(crate) operators: Vec<PhysicalOperator>,
    /// The sink (i.e. destination) for data; this is e.g. a hash table to-be-built
    pub(crate) sink: Option<PhysicalOperator>,

    /// The parent pipelines (i.e. pipelines that are dependent on this pipeline to finish)
    pub(crate) parents: Vec<Pipeline>,
    /// The dependencies of this pipeline
    pub(crate) dependencies: Vec<Pipeline>,

    /// The base batch index of this pipeline
    pub(crate) base_batch_index: usize,
}

impl Pipeline {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self {
            executor,
            ready: false,
            source: None,
            operators: vec![],
            sink: None,
            parents: vec![],
            dependencies: vec![],
            base_batch_index: 0,
        }
    }
}
