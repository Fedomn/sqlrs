use std::sync::Arc;

use derive_new::new;

use super::{BaseQueryResult, ClientContext, DatabaseError, MaterializedQueryResult, QueryResult};
use crate::execution::ExecutionContext;

#[derive(new)]
pub struct PendingQueryResult {
    pub(crate) client_context: Arc<ClientContext>,
}

impl PendingQueryResult {
    pub async fn execute(&self) -> Result<QueryResult, DatabaseError> {
        self.check_executable_internal().await?;

        let mut active_query_context = self.client_context.active_query.lock().await;
        let executor = active_query_context.executor.take().unwrap();
        let prepared = active_query_context.prepared.take().unwrap();
        // execute the query
        let execution_context = Arc::new(ExecutionContext::new(self.client_context.clone()));
        let collection = executor
            .try_execute(prepared.plan, execution_context)
            .await?;
        // set query result
        let materialized_query_result = MaterializedQueryResult::new(
            BaseQueryResult::new(prepared.types, prepared.names),
            collection,
        );
        Ok(QueryResult::MaterializedQueryResult(
            materialized_query_result,
        ))
    }

    async fn check_executable_internal(&self) -> Result<(), DatabaseError> {
        // whether the current pending query is active or not
        let invalidated = !self.client_context.is_active_request(self).await;
        if invalidated {
            return Err(DatabaseError::InternalError(
                "Attempting to execute an unsuccessful or closed pending query result".to_string(),
            ));
        }
        Ok(())
    }
}
