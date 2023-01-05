use super::Pipeline;
use crate::execution::ExecutionContext;

/// The Pipeline class represents an execution pipeline
pub struct PipelineExecutor {
    /// The pipeline to process
    pub(crate) pipeline: Pipeline,
    /// The total execution context of this executor
    pub(crate) context: ExecutionContext,
}
