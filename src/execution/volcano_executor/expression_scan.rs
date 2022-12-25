use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{
    BoxedExecutor, ExecutionContext, ExecutorError, ExpressionExecutor, PhysicalExpressionScan,
};

#[derive(new)]
pub struct ExpressionScan {
    pub(crate) plan: PhysicalExpressionScan,
    pub(crate) child: BoxedExecutor,
}

impl ExpressionScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let mut fields = vec![];
        for (idx, ty) in self.plan.expr_types.iter().enumerate() {
            fields.push(Field::new(
                format!("col{}", idx).as_str(),
                ty.clone().into(),
                true,
            ));
        }
        let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));

        #[for_await]
        for batch in self.child {
            let input = batch?;
            for exprs in self.plan.expressions.iter() {
                let columns = ExpressionExecutor::execute(exprs, &input)?;
                yield RecordBatch::try_new(schema.clone(), columns)?;
            }
        }
    }
}
