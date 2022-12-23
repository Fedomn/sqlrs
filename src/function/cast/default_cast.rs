use arrow::array::ArrayRef;
use arrow::compute::{cast_with_options, CastOptions};

use super::CastFunction;
use crate::function::FunctionError;
use crate::types_v2::LogicalType;

pub struct DefaultCastFunctions;

impl DefaultCastFunctions {
    fn default_cast_function(
        array: &ArrayRef,
        to_type: &LogicalType,
        try_cast: bool,
    ) -> Result<ArrayRef, FunctionError> {
        let to_type = to_type.clone().into();
        let options = CastOptions { safe: try_cast };
        Ok(cast_with_options(array, &to_type, &options)?)
    }

    pub fn get_cast_function(
        source: &LogicalType,
        target: &LogicalType,
    ) -> Result<CastFunction, FunctionError> {
        assert!(source != target);
        match source {
            LogicalType::Invalid => {
                Err(FunctionError::CastError("Invalid source type".to_string()))
            }
            _ => Ok(CastFunction::new(
                source.clone(),
                target.clone(),
                Self::default_cast_function,
            )),
        }
    }
}
