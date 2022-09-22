// most of ideas inspired by datafusion

use std::collections::HashSet;

use ahash::RandomState;
use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::compute;
use arrow::compute::kernels::cast::cast;
use arrow::datatypes::DataType;

use super::Accumulator;
use crate::executor::ExecutorError;
use crate::types::ScalarValue;

// returns the new value after sum with the new values, taking nullability into account
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let delta = compute::sum(array);
        ScalarValue::$SCALAR(delta)
    }};
}

// returns the sum of two scalar values, including coercion into $TYPE.
macro_rules! typed_sum {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        ScalarValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some(a + (*b as $TYPE)),
        })
    }};
}

pub struct SumAccumulator {
    result: ScalarValue,
    data_type: DataType,
}

impl SumAccumulator {
    pub fn new(data_type: DataType) -> Self {
        Self {
            result: ScalarValue::from(&data_type),
            data_type,
        }
    }

    fn sum_batch(
        &mut self,
        values: &ArrayRef,
        sum_type: &DataType,
    ) -> Result<ScalarValue, ExecutorError> {
        let values = cast(values, sum_type)?;
        Ok(match values.data_type() {
            DataType::Int32 => typed_sum_delta_batch!(values, Int32Array, Int32),
            DataType::Int64 => typed_sum_delta_batch!(values, Int64Array, Int64),
            DataType::Float64 => typed_sum_delta_batch!(values, Float64Array, Float64),
            _ => unimplemented!("unsupported sum type: {}", values.data_type()),
        })
    }
}

fn sum_result(l: &ScalarValue, r: &ScalarValue) -> ScalarValue {
    match (l, r) {
        // float64 coerces everything to f64
        (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        (ScalarValue::Float64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Float64, f64)
        }
        // i64 coerces i* to i64
        (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        (ScalarValue::Int64(lhs), ScalarValue::Int32(rhs)) => {
            typed_sum!(lhs, rhs, Int64, i64)
        }
        _ => unimplemented!("not expected {:?} and {:?} for sum", l, r),
    }
}

impl Accumulator for SumAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        let batch_sum_result = self.sum_batch(array, &self.data_type.clone())?;
        self.result = sum_result(&self.result, &batch_sum_result);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutorError> {
        Ok(self.result.clone())
    }
}

pub struct DistinctSumAccumulator {
    distinct_values: HashSet<ScalarValue, RandomState>,
    data_type: DataType,
}

impl DistinctSumAccumulator {
    pub fn new(data_type: DataType) -> Self {
        Self {
            distinct_values: HashSet::default(),
            data_type,
        }
    }
}

impl Accumulator for DistinctSumAccumulator {
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
        let mut sum = ScalarValue::from(&self.data_type);
        for v in self.distinct_values.iter() {
            sum = sum_result(&sum, v);
        }
        Ok(sum)
    }
}
