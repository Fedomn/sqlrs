use std::collections::HashSet;

use ahash::RandomState;
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

pub struct DistinctCountAccumulator {
    distinct_values: HashSet<ScalarValue, RandomState>,
}

impl DistinctCountAccumulator {
    pub fn new() -> Self {
        Self {
            distinct_values: HashSet::default(),
        }
    }
}

impl Accumulator for DistinctCountAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        if array.is_empty() {
            return Ok(());
        }
        (0..array.len()).for_each(|i| {
            let v = ScalarValue::try_from_array(array, i);
            self.distinct_values.insert(v);
        });
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutorError> {
        Ok(ScalarValue::Int64(Some(self.distinct_values.len() as i64)))
    }
}
