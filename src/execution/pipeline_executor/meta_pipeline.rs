use std::sync::Arc;

use super::{Executor, Pipeline};
use crate::execution::PhysicalOperator;

/// MetaPipeline represents a set of pipelines that all have the same sink
pub struct MetaPipeline {
    /// The executor for all MetaPipelines in the query plan
    pub(crate) executor: Arc<Executor>,
    /// The sink of all pipelines within this MetaPipeline
    pub(crate) sink: Option<PhysicalOperator>,
    /// All pipelines with a different source, but the same sink
    pub(crate) pipelines: Vec<Pipeline>,
    /// The pipelines that must finish before the MetaPipeline is finished
    pub(crate) final_pipelines: Vec<Pipeline>,
    /// Dependencies within this MetaPipeline
    pub(crate) dependencies: Vec<Pipeline>,
    /// Other MetaPipelines that this MetaPipeline depends on
    pub(crate) children: Vec<MetaPipeline>,
    pub(crate) next_batch_index: usize,
}

impl MetaPipeline {
    pub fn new(executor: Arc<Executor>, sink: Option<PhysicalOperator>) -> Self {
        let pipelines = vec![Pipeline::new(executor.clone())];
        Self {
            executor,
            sink: None,
            pipelines,
            final_pipelines: vec![],
            dependencies: vec![],
            children: vec![],
            next_batch_index: 0,
        }
    }

    pub fn create_pipeline(&mut self) -> Pipeline {
        let mut pipeline = Pipeline::new(self.executor.clone());
        pipeline.sink = self.sink.clone();
        // self.pipelines.push(pipeline.clone());
        pipeline
    }

    pub fn build(&mut self, op: &PhysicalOperator) {}

    pub(crate) fn create_child_meta_pipeline(
        &mut self,
        current: &Pipeline,
        sink: &PhysicalOperator,
    ) -> MetaPipeline {
        self.children
            .push(MetaPipeline::new(self.executor.clone(), Some(sink.clone())));

        todo!()
    }
}
