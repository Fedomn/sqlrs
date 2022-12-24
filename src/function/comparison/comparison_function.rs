use arrow::array::ArrayRef;
use derive_new::new;

use crate::function::FunctionError;
use crate::types_v2::LogicalType;

pub type ComparisonFunc = fn(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError>;

#[derive(new, Clone)]
pub struct ComparisonFunction {
    // The name of the function
    pub(crate) name: String,
    /// The main comparision function to execute.
    /// Left and right arguments must be the same type
    pub(crate) function: ComparisonFunc,
    /// The comparison type
    pub(crate) comparison_type: LogicalType,
}

impl std::fmt::Debug for ComparisonFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompressionFunction")
            .field("name", &self.name)
            .field(
                "func",
                &format!(
                    "{}{}{}",
                    self.comparison_type, self.name, self.comparison_type
                ),
            )
            .finish()
    }
}
