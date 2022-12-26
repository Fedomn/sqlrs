use arrow::array::ArrayRef;
use derive_new::new;
use strum_macros::AsRefStr;

use crate::function::FunctionError;

pub type ConjunctionFunc = fn(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef, FunctionError>;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, AsRefStr)]
pub enum ConjunctionType {
    And,
    Or,
}

#[derive(new, Clone)]
pub struct ConjunctionFunction {
    pub(crate) name: String,
    pub(crate) function: ConjunctionFunc,
    pub(crate) ty: ConjunctionType,
}

impl std::fmt::Debug for ConjunctionFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConjunctionFunction")
            .field("name", &self.name)
            .finish()
    }
}
