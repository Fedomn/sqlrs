use std::sync::Arc;

use arrow::array::{ArrayRef, *};
use arrow::compute::{
    add_checked, add_dyn_checked, divide_checked, multiply_checked, negate_checked,
    subtract_checked,
};
use arrow::datatypes::{DataType, IntervalUnit};

use super::ScalarFunction;
use crate::function::{BuiltinFunctions, FunctionError};
use crate::types_v2::LogicalType;

/// Invoke a compute kernel on array(s)
macro_rules! compute_op {
    // invoke binary operator
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $DT:ident) => {{
        let ll = $LEFT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        let rr = $RIGHT
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&ll, &rr)?))
    }};
    // invoke unary operator
    ($OPERAND:expr, $OP:ident, $DT:ident) => {{
        let operand = $OPERAND
            .as_any()
            .downcast_ref::<$DT>()
            .expect("compute_op failed to downcast array");
        Ok(Arc::new($OP(&operand)?))
    }};
}

/// Invoke a compute kernel on a pair of arrays
/// The binary_primitive_array_op macro only evaluates for primitive types
/// like integers and floats.
macro_rules! binary_primitive_array_op {
    ($LEFT:expr, $RIGHT:expr, $OP:ident) => {{
        match $LEFT.data_type() {
            DataType::Int8 => compute_op!($LEFT, $RIGHT, $OP, Int8Array),
            DataType::Int16 => compute_op!($LEFT, $RIGHT, $OP, Int16Array),
            DataType::Int32 => compute_op!($LEFT, $RIGHT, $OP, Int32Array),
            DataType::Int64 => compute_op!($LEFT, $RIGHT, $OP, Int64Array),
            DataType::UInt8 => compute_op!($LEFT, $RIGHT, $OP, UInt8Array),
            DataType::UInt16 => compute_op!($LEFT, $RIGHT, $OP, UInt16Array),
            DataType::UInt32 => compute_op!($LEFT, $RIGHT, $OP, UInt32Array),
            DataType::UInt64 => compute_op!($LEFT, $RIGHT, $OP, UInt64Array),
            DataType::Float32 => compute_op!($LEFT, $RIGHT, $OP, Float32Array),
            DataType::Float64 => compute_op!($LEFT, $RIGHT, $OP, Float64Array),
            other => Err(FunctionError::InternalError(format!(
                "Data type {:?} not supported for binary operation '{}' on primitive arrays",
                other,
                stringify!($OP)
            ))),
        }
    }};
}

pub struct AddFunction;

impl AddFunction {
    fn add(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        binary_primitive_array_op!(left, right, add_checked)
    }

    fn date_add_interval_func(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        Ok(add_dyn_checked(left, right)?)
    }

    fn interval_add_date_func(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        Ok(add_dyn_checked(right, left)?)
    }

    fn gen_date_funcs() -> Vec<ScalarFunction> {
        let mut functions = vec![];
        let args1 = [
            [
                LogicalType::Date,
                LogicalType::Interval(IntervalUnit::YearMonth),
            ],
            [
                LogicalType::Date,
                LogicalType::Interval(IntervalUnit::DayTime),
            ],
        ];
        for arg in args1.iter() {
            functions.push(ScalarFunction::new(
                "add".to_string(),
                Self::date_add_interval_func,
                arg.to_vec(),
                LogicalType::Date,
            ));
        }
        let args2 = [
            [
                LogicalType::Interval(IntervalUnit::YearMonth),
                LogicalType::Date,
            ],
            [
                LogicalType::Interval(IntervalUnit::DayTime),
                LogicalType::Date,
            ],
        ];
        for arg in args2.iter() {
            functions.push(ScalarFunction::new(
                "add".to_string(),
                Self::interval_add_date_func,
                arg.to_vec(),
                LogicalType::Date,
            ));
        }
        functions
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        let mut functions = vec![];
        for ty in LogicalType::numeric().iter() {
            functions.push(ScalarFunction::new(
                "add".to_string(),
                Self::add,
                vec![ty.clone(), ty.clone()],
                ty.clone(),
            ));
        }
        functions.extend(Self::gen_date_funcs());
        set.add_scalar_functions("add".to_string(), functions)?;
        Ok(())
    }
}

pub struct SubtractFunction;

impl SubtractFunction {
    fn subtract(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        binary_primitive_array_op!(left, right, subtract_checked)
    }

    fn negate_interval(input: &ArrayRef) -> Result<ArrayRef, FunctionError> {
        match input.data_type() {
            DataType::Interval(IntervalUnit::YearMonth) => {
                compute_op!(input, negate_checked, IntervalYearMonthArray)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                compute_op!(input, negate_checked, IntervalDayTimeArray)
            }
            other => Err(FunctionError::InternalError(format!(
                "Data type {:?} not supported for negate",
                other
            ))),
        }
    }

    fn date_subtract_interval_func(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        let right = Self::negate_interval(right)?;
        Ok(add_dyn_checked(left, &right)?)
    }

    fn gen_date_funcs() -> Vec<ScalarFunction> {
        let mut functions = vec![];
        let args1 = [
            [
                LogicalType::Date,
                LogicalType::Interval(IntervalUnit::YearMonth),
            ],
            [
                LogicalType::Date,
                LogicalType::Interval(IntervalUnit::DayTime),
            ],
        ];
        for arg in args1.iter() {
            functions.push(ScalarFunction::new(
                "subtract".to_string(),
                Self::date_subtract_interval_func,
                arg.to_vec(),
                LogicalType::Date,
            ));
        }
        functions
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        let mut functions = vec![];
        for ty in LogicalType::numeric().iter() {
            functions.push(ScalarFunction::new(
                "subtract".to_string(),
                Self::subtract,
                vec![ty.clone(), ty.clone()],
                ty.clone(),
            ));
        }
        functions.extend(Self::gen_date_funcs());
        set.add_scalar_functions("subtract".to_string(), functions.clone())?;
        Ok(())
    }
}

pub struct MultiplyFunction;

impl MultiplyFunction {
    fn multiply(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        binary_primitive_array_op!(left, right, multiply_checked)
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        let mut functions = vec![];
        for ty in LogicalType::numeric().iter() {
            functions.push(ScalarFunction::new(
                "multiply".to_string(),
                Self::multiply,
                vec![ty.clone(), ty.clone()],
                ty.clone(),
            ));
        }
        set.add_scalar_functions("multiply".to_string(), functions.clone())?;
        Ok(())
    }
}

pub struct DivideFunction;

impl DivideFunction {
    fn divide(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError> {
        assert!(inputs.len() == 2);
        let left = &inputs[0];
        let right = &inputs[1];
        binary_primitive_array_op!(left, right, divide_checked)
    }

    pub fn register_function(set: &mut BuiltinFunctions) -> Result<(), FunctionError> {
        let mut functions = vec![];
        for ty in LogicalType::numeric().iter() {
            functions.push(ScalarFunction::new(
                "divide".to_string(),
                Self::divide,
                vec![ty.clone(), ty.clone()],
                ty.clone(),
            ));
        }
        set.add_scalar_functions("divide".to_string(), functions.clone())?;
        Ok(())
    }
}
