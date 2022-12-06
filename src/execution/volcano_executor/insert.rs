use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{
    BoxedExecutor, ExecutionContext, ExecutorError, ExpressionExecutor, PhysicalInsert,
};
use crate::planner_v2::{
    BoundConstantExpression, BoundExpression, BoundExpressionBase, BoundReferenceExpression,
    INVALID_INDEX,
};
use crate::storage_v2::LocalStorage;
use crate::types_v2::ScalarValue;

#[derive(new)]
pub struct Insert {
    pub(crate) plan: PhysicalInsert,
    pub(crate) child: BoxedExecutor,
}

impl Insert {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, context: Arc<ExecutionContext>) {
        let table = self.plan.table.storage;
        let mut exprs = vec![];
        let mut fields = vec![];
        for (table_col_idx, col_insert_idx) in self.plan.column_index_list.iter().enumerate() {
            let column = table.column_definitions[table_col_idx].clone();
            fields.push(Field::new(
                column.name.as_str(),
                column.ty.clone().into(),
                true,
            ));
            let ty = column.ty.clone();
            let base = BoundExpressionBase::new("".to_string(), ty.clone());
            if *col_insert_idx == INVALID_INDEX {
                let value = ScalarValue::new_none_value(&ty.into())?;
                let expr = BoundExpression::BoundConstantExpression(BoundConstantExpression::new(
                    base, value,
                ));
                exprs.push(expr);
            } else {
                let expr = BoundExpression::BoundReferenceExpression(
                    BoundReferenceExpression::new(base, *col_insert_idx),
                );
                exprs.push(expr);
            }
        }
        let schema = SchemaRef::new(Schema::new_with_metadata(fields.clone(), HashMap::new()));
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let columns = ExpressionExecutor::execute(&exprs, &batch)?;
            let chunk = RecordBatch::try_new(schema.clone(), columns)?;
            LocalStorage::append(context.clone_client_context(), &table, chunk);
        }
    }
}
