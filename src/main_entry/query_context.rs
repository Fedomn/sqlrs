use std::sync::Arc;

use super::{PendingQueryResult, PreparedStatementData};
use crate::execution::VolcanoExecutor;

#[derive(Default)]
pub struct ActiveQueryContext {
    /// The query that is currently being executed
    pub(crate) query: Option<String>,
    /// The currently open result
    pub(crate) open_result: Option<Arc<PendingQueryResult>>,
    /// Prepared statement data
    pub(crate) prepared: Option<PreparedStatementData>,
    /// The query executor
    pub(crate) executor: Option<VolcanoExecutor>,
}

impl ActiveQueryContext {
    pub fn reset(&mut self) {
        self.query = None;
        self.open_result = None;
        self.prepared = None;
        self.executor = None;
    }

    pub fn is_empty(&self) -> bool {
        self.query.is_none()
            && self.open_result.is_none()
            && self.prepared.is_none()
            && self.executor.is_none()
    }
}
