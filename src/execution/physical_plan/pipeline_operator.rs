use arrow::record_batch::RecordBatch;

use super::state::{GlobalSinkState, LocalSinkState};
use super::{
    GlobalOperatorState, GlobalSourceState, LocalSourceState, OperatorFinalizeResultType,
    OperatorResultType, OperatorState, SinkResultType,
};
use crate::execution::ExecutionContext;

pub trait PipelineSource {
    // source interface
    fn get_local_source_state(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
    ) -> Box<dyn LocalSourceState>;

    fn get_global_source_state(&self, context: &ExecutionContext) -> Box<dyn GlobalSourceState>;

    fn get_data(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalSourceState,
        lstate: &dyn LocalSourceState,
    ) -> RecordBatch;

    fn parallel_source(&self) -> bool {
        false
    }
}

pub trait PipelineOperator {
    // operator interface
    fn get_operator_state(&self, context: &ExecutionContext) -> Box<dyn OperatorState>;

    fn get_global_operator_state(&self, context: &ExecutionContext)
        -> Box<dyn GlobalOperatorState>;

    fn execute(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalOperatorState,
        lstate: &dyn OperatorState,
        input: &RecordBatch,
    ) -> (OperatorResultType, RecordBatch);

    fn final_execute(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalOperatorState,
        lstate: &dyn OperatorState,
    ) -> (OperatorFinalizeResultType, RecordBatch);

    fn parallel_operator(&self) -> bool {
        false
    }

    fn requires_final_execute(&self) -> bool {
        false
    }
}

pub trait PipelineSink {
    fn get_local_sink_state(&self, context: &ExecutionContext) -> Box<dyn LocalSinkState>;

    fn get_global_sink_state(&self, context: &ExecutionContext) -> Box<dyn GlobalSinkState>;

    /// The sink method is called constantly with new input, as long as new input is available. Note
    /// that this method CAN be called in parallel, proper locking is needed when accessing data
    /// inside the GlobalSinkState.
    fn sink(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalSinkState,
        lstate: &dyn LocalSinkState,
    ) -> (SinkResultType, RecordBatch);

    // The combine is called when a single thread has completed execution of its part of the
    // pipeline, it is the final time that a specific LocalSinkState is accessible. This method
    // can be called in parallel while other Sink() or Combine() calls are active on the same
    // GlobalSinkState.
    fn combine(
        &self,
        context: &ExecutionContext,
        gstate: &dyn GlobalSinkState,
        lstate: &dyn LocalSinkState,
    );

    fn finalize(&self, context: &ExecutionContext, gstate: &dyn GlobalSinkState);

    fn parallel_sink(&self) -> bool {
        false
    }
}
