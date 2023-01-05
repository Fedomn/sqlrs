use super::SinkFinalizeType;
use crate::planner_v2::INVALID_INDEX;

pub trait OperatorState {}

pub trait GlobalOperatorState {}

pub trait GlobalSinkState {
    fn finalize_type(&self) -> SinkFinalizeType {
        SinkFinalizeType::Ready
    }
}

pub trait LocalSinkState {
    //! The current batch index
    //! This is only set in case RequiresBatchIndex() is true, and the source has support for it (SupportsBatchIndex())
    //! Otherwise this is left on INVALID_INDEX
    //! The batch index is a globally unique, increasing index that should be used to maintain insertion order
    //! //! in conjunction with parallelism
    fn batch_index(&self) -> usize {
        INVALID_INDEX
    }
}

pub trait GlobalSourceState {
    fn max_threads(&self) -> usize {
        1
    }
}

pub trait LocalSourceState {}
