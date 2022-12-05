use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use futures::lock::Mutex;
use sqlparser::ast::Statement;

use super::query_context::ActiveQueryContext;
use super::{
    DatabaseError, DatabaseInstance, PendingQueryResult, PreparedStatementData, QueryResult,
};
use crate::execution::{PhysicalPlanGenerator, VolcanoExecutor};
use crate::parser::Sqlparser;
use crate::planner_v2::Planner;
use crate::util::pretty_batches_with;

/// The ClientContext holds information relevant to the current client session during execution
pub struct ClientContext {
    /// The database that this client is connected to
    pub(crate) db: Arc<DatabaseInstance>,
    pub(crate) active_query: Mutex<ActiveQueryContext>,
    pub(crate) interrupted: AtomicBool,
}

impl ClientContext {
    pub fn new(db: Arc<DatabaseInstance>) -> Arc<Self> {
        Arc::new(Self {
            db,
            active_query: Mutex::new(ActiveQueryContext::default()),
            interrupted: AtomicBool::new(false),
        })
    }

    pub async fn query(self: &Arc<Self>, sql: String) -> Result<Vec<RecordBatch>, DatabaseError> {
        let statements = Sqlparser::parse(sql.clone())?;
        if statements.is_empty() {
            return Err(DatabaseError::InternalError(
                "invalid statement".to_string(),
            ));
        }

        let mut collection_result = vec![];

        for stat in statements.iter() {
            let result = self.pending_query(stat).await?;
            match result {
                QueryResult::MaterializedQueryResult(res) => {
                    pretty_batches_with(&res.collection, &res.base.names, &res.base.types);
                    collection_result.extend(res.collection);
                }
            }
        }

        Ok(collection_result)
    }

    async fn pending_query(
        self: &Arc<Self>,
        statement: &Statement,
    ) -> Result<QueryResult, DatabaseError> {
        let pending_query = self
            .pending_statement_or_prepared_statement(statement)
            .await?;
        let result = pending_query.execute().await?;
        Ok(result)
    }

    async fn pending_statement_or_prepared_statement(
        self: &Arc<Self>,
        statement: &Statement,
    ) -> Result<Arc<PendingQueryResult>, DatabaseError> {
        self.initial_cleanup().await;

        self.active_query.lock().await.query = Some(statement.to_string());
        // prepare the query for execution
        let prepared = self.create_prepared_statement(statement).await?;
        self.active_query.lock().await.prepared = Some(prepared);
        // set volcano executor
        let executor = VolcanoExecutor::new();
        self.active_query.lock().await.executor = Some(executor);
        // return pending query result
        let pending_query_result = Arc::new(PendingQueryResult::new(self.clone()));
        self.active_query.lock().await.open_result = Some(pending_query_result.clone());
        Ok(pending_query_result.clone())
    }

    async fn create_prepared_statement(
        self: &Arc<Self>,
        statement: &Statement,
    ) -> Result<PreparedStatementData, DatabaseError> {
        let mut planner = Planner::new(self.clone());
        planner.create_plan(statement)?;
        let logical_plan = planner.plan.unwrap();
        let names = planner.names.unwrap();
        let types = planner.types.unwrap();

        let physical_planner = PhysicalPlanGenerator::new(self.clone());
        let physical_plan = physical_planner.create_plan(logical_plan);

        let result = PreparedStatementData::new(statement.clone(), physical_plan, names, types);
        Ok(result)
    }

    async fn initial_cleanup(self: &Arc<Self>) {
        self.cleanup_internal().await;
        self.interrupted.store(false, Ordering::Release);
    }

    async fn cleanup_internal(self: &Arc<Self>) {
        self.active_query.lock().await.reset();
    }

    pub async fn is_active_request(self: &Arc<Self>, query_result: &PendingQueryResult) -> bool {
        let active_query_context = self.active_query.lock().await;
        if active_query_context.is_empty() {
            return false;
        }
        if let Some(open_result) = &active_query_context.open_result {
            std::ptr::eq(open_result.as_ref(), query_result)
        } else {
            false
        }
    }
}
