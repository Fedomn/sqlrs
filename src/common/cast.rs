use arrow::array::{Array, BooleanArray};

use crate::function::FunctionError;

/// Downcast an Arrow Array to a concrete type
macro_rules! downcast_value {
    ($Value:expr, $Type:ident) => {{
        use std::any::type_name;
        $Value.as_any().downcast_ref::<$Type>().ok_or_else(|| {
            FunctionError::CastError(format!("could not cast value to {}", type_name::<$Type>()))
        })?
    }};
}

/// Downcast ArrayRef to BooleanArray
pub fn as_boolean_array(array: &dyn Array) -> Result<&BooleanArray, FunctionError> {
    Ok(downcast_value!(array, BooleanArray))
}
