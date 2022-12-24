use arrow::array::ArrayRef;
use derive_new::new;

use crate::function::FunctionError;

pub type ConjunctionFunc = fn(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError>;

#[derive(new, Clone)]
pub struct ConjunctionFunction {
    pub(crate) name: String,
    pub(crate) function: ConjunctionFunc,
}

impl std::fmt::Debug for ConjunctionFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConjunctionFunction")
            .field("name", &self.name)
            .finish()
    }
}
