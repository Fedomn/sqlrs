mod create_table;
mod expression_scan;
mod insert;
mod projection;
mod table_scan;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
pub use create_table::*;
pub use expression_scan::*;
use futures::stream::BoxStream;
use futures::TryStreamExt;
pub use insert::*;
pub use projection::*;
pub use table_scan::*;

use super::{ExecutionContext, ExecutorError, PhysicalOperator};

pub type BoxedExecutor = BoxStream<'static, Result<RecordBatch, ExecutorError>>;

#[derive(Default)]
pub struct VolcanoExecutor {}

impl VolcanoExecutor {
    pub fn new() -> Self {
        VolcanoExecutor::default()
    }

    fn build(&self, plan: PhysicalOperator, context: Arc<ExecutionContext>) -> BoxedExecutor {
        match plan {
            PhysicalOperator::PhysicalCreateTable(op) => CreateTable::new(op).execute(context),
            PhysicalOperator::PhysicalExpressionScan(op) => {
                ExpressionScan::new(op).execute(context)
            }
            PhysicalOperator::PhysicalInsert(op) => {
                let child = op.base.children.first().unwrap().clone();
                let child_executor = self.build(child, context.clone());
                Insert::new(op, child_executor).execute(context)
            }
            PhysicalOperator::PhysicalTableScan(op) => TableScan::new(op).execute(context),
            PhysicalOperator::PhysicalProjection(op) => {
                let child = op.base.children.first().unwrap().clone();
                let child_executor = self.build(child, context.clone());
                Projection::new(op, child_executor).execute(context)
            }
        }
    }

    pub(crate) async fn try_execute(
        &self,
        plan: PhysicalOperator,
        context: Arc<ExecutionContext>,
    ) -> Result<Vec<RecordBatch>, ExecutorError> {
        let mut output = Vec::new();
        let mut volcano_executor = self.build(plan, context.clone());
        while let Some(batch) = volcano_executor.try_next().await? {
            output.push(batch);
        }
        Ok(output)
    }
}
