// arrow compute logic inspired by datafusion

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::compute;
use arrow::datatypes::DataType;

use super::Accumulator;
use crate::executor::ExecutorError;
use crate::types::ScalarValue;

// Statically-typed version of min/max(array) -> ScalarValue for string types.
macro_rules! typed_min_max_batch_string {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        let value = value.and_then(|e| Some(e.to_string()));
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue for non-string types.
macro_rules! typed_min_max_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident, $OP:ident) => {{
        let array = $VALUES.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        let value = compute::$OP(array);
        ScalarValue::$SCALAR(value)
    }};
}

// Statically-typed version of min/max(array) -> ScalarValue  for non-string types.
// this is a macro to support both operations (min and max).
macro_rules! min_max_batch {
    ($VALUES:expr, $OP:ident) => {{
        match $VALUES.data_type() {
            // all types that have a natural order
            DataType::Float64 => {
                typed_min_max_batch!($VALUES, Float64Array, Float64, $OP)
            }
            DataType::Int32 => typed_min_max_batch!($VALUES, Int32Array, Int32, $OP),
            DataType::Int64 => typed_min_max_batch!($VALUES, Int64Array, Int64, $OP),
            _ => unimplemented!("unsupported min/max type: {}", $VALUES.data_type()),
        }
    }};
}

/// dynamically-typed min(array) -> ScalarValue
fn min_batch(values: &ArrayRef) -> Result<ScalarValue, ExecutorError> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, String, min_string)
        }
        _ => min_max_batch!(values, min),
    })
}

/// dynamically-typed max(array) -> ScalarValue
fn max_batch(values: &ArrayRef) -> Result<ScalarValue, ExecutorError> {
    Ok(match values.data_type() {
        DataType::Utf8 => {
            typed_min_max_batch_string!(values, StringArray, String, max_string)
        }
        _ => min_max_batch!(values, max),
    })
}

// min/max of two scalar string values.
macro_rules! typed_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        })
    }};
}

// min/max of two non-string scalar values.
macro_rules! typed_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        ScalarValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((*a).$OP(*b)),
        })
    }};
}

// min/max of two scalar values of the same type
macro_rules! min_max {
    ($VALUE:expr, $DELTA:expr, $OP:ident) => {{
        match ($VALUE, $DELTA) {
            (ScalarValue::Float64(lhs), ScalarValue::Float64(rhs)) => {
                typed_min_max!(lhs, rhs, Float64, $OP)
            }
            (ScalarValue::Int64(lhs), ScalarValue::Int64(rhs)) => {
                typed_min_max!(lhs, rhs, Int64, $OP)
            }
            (ScalarValue::Int32(lhs), ScalarValue::Int32(rhs)) => {
                typed_min_max!(lhs, rhs, Int32, $OP)
            }
            (ScalarValue::String(lhs), ScalarValue::String(rhs)) => {
                typed_min_max_string!(lhs, rhs, String, $OP)
            }
            _ => unimplemented!("unsupported min_max scalar type: {}", $VALUE.data_type()),
        }
    }};
}

pub struct MinAccumulator {
    min: ScalarValue,
}

impl MinAccumulator {
    pub fn new(data_type: DataType) -> Self {
        Self {
            min: ScalarValue::from(&data_type),
        }
    }
}

impl Accumulator for MinAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        let delta = &min_batch(array)?;
        self.min = min_max!(&self.min, delta, min);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutorError> {
        Ok(self.min.clone())
    }
}

pub struct MaxAccumulator {
    max: ScalarValue,
}

impl MaxAccumulator {
    pub fn new(data_type: DataType) -> Self {
        Self {
            max: ScalarValue::from(&data_type),
        }
    }
}

impl Accumulator for MaxAccumulator {
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError> {
        let delta = &max_batch(array)?;
        self.max = min_max!(&self.max, delta, max);
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue, ExecutorError> {
        Ok(self.max.clone())
    }
}
