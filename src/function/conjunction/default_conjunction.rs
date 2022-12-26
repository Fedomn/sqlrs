use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray};
use arrow::compute::{and_kleene, or_kleene};
use arrow::datatypes::DataType;
use sqlparser::ast::BinaryOperator;

use super::{ConjunctionFunc, ConjunctionFunction, ConjunctionType};
use crate::function::FunctionError;

pub struct DefaultConjunctionFunctions;

macro_rules! boolean_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        if *$LEFT.data_type() != DataType::Boolean || *$RIGHT.data_type() != DataType::Boolean {
            return Err(FunctionError::ConjunctionError(format!(
                "Cannot evaluate binary expression with types {:?} and {:?}, only Boolean supported",
                $LEFT.data_type(),
                $RIGHT.data_type()
            )));
        }

        let ll = $LEFT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("boolean_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
}

impl DefaultConjunctionFunctions {
    fn default_and_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        boolean_op!(left, right, and_kleene)
    }

    fn default_or_function(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        boolean_op!(left, right, or_kleene)
    }

    fn get_conjunction_function_internal(
        op: &BinaryOperator,
    ) -> Result<(ConjunctionType, ConjunctionFunc), FunctionError> {
        Ok(match op {
            BinaryOperator::And => (ConjunctionType::And, Self::default_and_function),
            BinaryOperator::Or => (ConjunctionType::Or, Self::default_or_function),
            _ => {
                return Err(FunctionError::ConjunctionError(format!(
                    "Unsupported conjunction operator {:?}",
                    op
                )))
            }
        })
    }

    pub fn get_conjunction_function(
        op: &BinaryOperator,
    ) -> Result<ConjunctionFunction, FunctionError> {
        let (ty, func) = Self::get_conjunction_function_internal(op)?;
        Ok(ConjunctionFunction::new(ty.as_ref().to_string(), func, ty))
    }
}
