use core::fmt;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use ordered_float::OrderedFloat;

macro_rules! typed_cast {
    ($array:expr, $index:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = $array.as_any().downcast_ref::<$ARRAYTYPE>().unwrap();
        ScalarValue::$SCALAR(match array.is_null($index) {
            true => None,
            false => Some(array.value($index).into()),
        })
    }};
}

/// To keep simplicity, we only support some scalar value
/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part of arrow’s `Array`.
#[derive(Clone, Debug)]
pub enum ScalarValue {
    /// represents `DataType::Null` (castable to/from any other type)
    Null,
    /// true or false value
    Boolean(Option<bool>),
    /// 64bit float
    Float64(Option<f64>),
    /// signed 32bit int
    Int32(Option<i32>),
    /// signed 64bit int
    Int64(Option<i64>),
    /// utf-8 encoded string.
    String(Option<String>),
}

impl ScalarValue {
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::String(_) => DataType::Utf8,
        }
    }

    pub fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => ScalarValue::Boolean(None),
            DataType::Float64 => ScalarValue::Float64(None),
            DataType::Int32 => ScalarValue::Int32(None),
            DataType::Int64 => ScalarValue::Int64(None),
            DataType::Utf8 => ScalarValue::String(None),
            _ => panic!("Unsupported data type: {}", data_type),
        }
    }

    /// Converts a value in `array` at `index` into a ScalarValue
    pub fn try_from_array(array: &ArrayRef, index: usize) -> Self {
        // handle NULL value
        if !array.is_valid(index) {
            return Self::from(array.data_type());
        }

        match array.data_type() {
            DataType::Null => ScalarValue::Null,
            DataType::Boolean => typed_cast!(array, index, BooleanArray, Boolean),
            DataType::Float64 => typed_cast!(array, index, Float64Array, Float64),
            DataType::Int64 => typed_cast!(array, index, Int64Array, Int64),
            DataType::Int32 => typed_cast!(array, index, Int32Array, Int32),
            DataType::Utf8 => typed_cast!(array, index, StringArray, String),
            _ => panic!("Unsupported data type: {}", array.data_type()),
        }
    }

    pub fn as_usize(&self) -> Option<usize> {
        match self {
            ScalarValue::Int64(Some(v)) => Some(*v as usize),
            ScalarValue::Int32(Some(v)) => Some(*v as usize),
            _ => None,
        }
    }

    /// This method is to eliminate unnecessary type conversion
    /// TODO: enhance this to support more types
    pub fn cast_to_type(&self, cast_type: &DataType) -> Option<ScalarValue> {
        match (self, cast_type) {
            (ScalarValue::Int32(v), DataType::Int64) => {
                v.map(|v| ScalarValue::Int64(Some(v as i64)))
            }
            (ScalarValue::Int32(v), DataType::Float64) => {
                v.map(|v| ScalarValue::Float64(Some(v as f64)))
            }
            _ => None,
        }
    }
}

macro_rules! format_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "Null"),
            ScalarValue::Boolean(v) => format_option!(f, v),
            ScalarValue::Float64(v) => format_option!(f, v),
            ScalarValue::Int32(v) => format_option!(f, v),
            ScalarValue::Int64(v) => format_option!(f, v),
            ScalarValue::String(v) => format_option!(f, v),
        }
    }
}

macro_rules! impl_scalar {
    ($ty:ty, $scalar:tt) => {
        impl From<$ty> for ScalarValue {
            fn from(value: $ty) -> Self {
                ScalarValue::$scalar(Some(value))
            }
        }

        impl From<Option<$ty>> for ScalarValue {
            fn from(value: Option<$ty>) -> Self {
                ScalarValue::$scalar(value)
            }
        }
    };
}

impl_scalar!(f64, Float64);
impl_scalar!(i32, Int32);
impl_scalar!(i64, Int64);
impl_scalar!(bool, Boolean);
impl_scalar!(String, String);

impl From<&sqlparser::ast::Value> for ScalarValue {
    fn from(v: &sqlparser::ast::Value) -> Self {
        match v {
            sqlparser::ast::Value::Number(n, _) => {
                if let Ok(int) = n.parse::<i32>() {
                    int.into()
                } else if let Ok(bigint) = n.parse::<i64>() {
                    bigint.into()
                } else if let Ok(float) = n.parse::<f64>() {
                    float.into()
                } else {
                    todo!("unsupported number {:?}", n)
                }
            }
            sqlparser::ast::Value::SingleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::DoubleQuotedString(s) => s.clone().into(),
            sqlparser::ast::Value::Boolean(b) => (*b).into(),
            sqlparser::ast::Value::Null => Self::Null,
            _ => todo!("unsupported parsed scalar value {:?}", v),
        }
    }
}

impl PartialEq for ScalarValue {
    fn eq(&self, other: &Self) -> bool {
        use ScalarValue::*;

        match (self, other) {
            (Null, Null) => true,
            (Null, _) => false,
            (Boolean(v1), Boolean(v2)) => v1.eq(v2),
            (Boolean(_), _) => false,
            (Float64(v1), Float64(v2)) => {
                let v1 = v1.map(OrderedFloat);
                let v2 = v2.map(OrderedFloat);
                v1.eq(&v2)
            }
            (Float64(_), _) => false,
            (Int32(v1), Int32(v2)) => v1.eq(v2),
            (Int32(_), _) => false,
            (Int64(v1), Int64(v2)) => v1.eq(v2),
            (Int64(_), _) => false,
            (String(v1), String(v2)) => v1.eq(v2),
            (String(_), _) => false,
        }
    }
}

impl Eq for ScalarValue {}

impl std::hash::Hash for ScalarValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            // stable hash for Null value
            ScalarValue::Null => 1.hash(state),
            ScalarValue::Boolean(v) => v.hash(state),
            ScalarValue::Float64(v) => {
                // f64 not implement Hash, see https://internals.rust-lang.org/t/f32-f64-should-implement-hash/5436/3
                v.map(OrderedFloat).hash(state);
            }
            ScalarValue::Int32(v) => v.hash(state),
            ScalarValue::Int64(v) => v.hash(state),
            ScalarValue::String(v) => v.hash(state),
        }
    }
}

pub fn build_scalar_value_array(scalar_value: &ScalarValue, capacity: usize) -> ArrayRef {
    match scalar_value {
        ScalarValue::Null => new_null_array(&DataType::Null, capacity),
        ScalarValue::Boolean(b) => Arc::new(BooleanArray::from(vec![*b; capacity])),
        ScalarValue::Float64(f) => Arc::new(Float64Array::from(vec![*f; capacity])),
        ScalarValue::Int32(i) => Arc::new(Int32Array::from(vec![*i; capacity])),
        ScalarValue::Int64(i) => Arc::new(Int64Array::from(vec![*i; capacity])),
        ScalarValue::String(s) => Arc::new(StringArray::from(vec![s.as_deref(); capacity])),
    }
}

pub fn build_scalar_value_builder(data_type: &DataType) -> Box<dyn ArrayBuilder> {
    match data_type {
        DataType::Boolean => Box::new(BooleanBuilder::new()),
        DataType::Float64 => Box::new(Float64Builder::new()),
        DataType::Int32 => Box::new(Int32Builder::new()),
        DataType::Int64 => Box::new(Int64Builder::new()),
        DataType::Utf8 => Box::new(StringBuilder::new()),
        _ => panic!("Unsupported data type: {}", data_type),
    }
}

pub fn append_scalar_value_for_builder(
    scalar_value: &ScalarValue,
    builder: &mut Box<dyn ArrayBuilder>,
) -> Result<(), ArrowError> {
    match scalar_value {
        ScalarValue::Null => {
            return Err(ArrowError::NotYetImplemented(
                "not support Null as group by key".to_string(),
            ))
        }
        ScalarValue::Boolean(v) => builder
            .as_any_mut()
            .downcast_mut::<BooleanBuilder>()
            .unwrap()
            .append_option(*v),
        ScalarValue::Float64(v) => builder
            .as_any_mut()
            .downcast_mut::<Float64Builder>()
            .unwrap()
            .append_option(*v),
        ScalarValue::Int32(v) => builder
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap()
            .append_option(*v),
        ScalarValue::Int64(v) => builder
            .as_any_mut()
            .downcast_mut::<Int64Builder>()
            .unwrap()
            .append_option(*v),
        ScalarValue::String(v) => builder
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap()
            .append_option(v.as_ref()),
    }
    Ok(())
}
