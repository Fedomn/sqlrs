mod column_binding_resolver;
mod expression_executor;
mod physical_plan;
mod physical_plan_generator;
mod volcano_executor;
use std::sync::Arc;
mod util;

use arrow::error::ArrowError;
pub use column_binding_resolver::*;
use derive_new::new;
pub use expression_executor::*;
pub use physical_plan::*;
pub use physical_plan_generator::*;
pub use util::*;
pub use volcano_executor::*;

use crate::catalog_v2::CatalogError;
use crate::main_entry::ClientContext;
use crate::types_v2::TypeError;

#[derive(new)]
pub struct ExecutionContext {
    pub(crate) client_context: Arc<ClientContext>,
}

impl ExecutionContext {
    pub fn clone_client_context(&self) -> Arc<ClientContext> {
        self.client_context.clone()
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ExecutorError {
    #[error("catalog error: {0}")]
    CatalogError(
        #[source]
        #[from]
        CatalogError,
    ),
    #[error("arrow error: {0}")]
    ArrowError(
        #[source]
        #[from]
        ArrowError,
    ),
    #[error("type error: {0}")]
    TypeError(
        #[source]
        #[from]
        TypeError,
    ),
    #[error("Executor internal error: {0}")]
    InternalError(String),
}
