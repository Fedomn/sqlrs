use arrow::array::ArrayRef;
use derive_new::new;

use crate::function::FunctionError;
use crate::types_v2::LogicalType;

pub type CastFunc =
    fn(array: &ArrayRef, to_type: &LogicalType, try_cast: bool) -> Result<ArrayRef, FunctionError>;

#[derive(new, Clone)]
pub struct CastFunction {
    /// The source type of the cast
    pub(crate) source: LogicalType,
    /// The target type of the cast
    pub(crate) target: LogicalType,
    /// The main cast function to execute
    pub(crate) function: CastFunc,
}

impl std::fmt::Debug for CastFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CastFunction")
            .field("cast", &format!("{:?} -> {:?}", self.source, self.target))
            .finish()
    }
}
