use core::fmt;
use std::sync::Arc;

use arrow::array::{
    new_null_array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::DataType;

/// To keep simplicity, we only support some scalar value
/// Represents a dynamically typed, nullable single value.
/// This is the single-valued counter-part of arrow’s `Array`.
#[derive(Clone, Debug, PartialEq)]
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
