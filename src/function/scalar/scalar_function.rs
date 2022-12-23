use arrow::array::ArrayRef;
use derive_new::new;

use crate::function::FunctionError;
use crate::types_v2::LogicalType;

pub type ScalarFunc = fn(inputs: &[ArrayRef]) -> Result<ArrayRef, FunctionError>;

#[derive(new, Clone)]
pub struct ScalarFunction {
    // The name of the function
    pub(crate) name: String,
    /// The main scalar function to execute
    pub(crate) function: ScalarFunc,
    /// The set of arguments of the function
    pub(crate) arguments: Vec<LogicalType>,
    /// Return type of the function
    pub(crate) return_type: LogicalType,
}

impl std::fmt::Debug for ScalarFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScalarFunction")
            .field("name", &self.name)
            .field(
                "types",
                &format!("{:?} -> {:?}", self.arguments, self.return_type),
            )
            .finish()
    }
}
