use arrow::array::ArrayRef;

use super::Accumulator;
use crate::executor::ExecutorError;
use crate::types::ScalarValue;

pub struct CountAccumulator {
    result: i64,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self { result: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        self.result = (array.len() - array.null_count()) as i64;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutorError> {
        Ok(ScalarValue::Int64(Some(self.result)))
    }
}
