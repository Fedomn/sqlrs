use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalDummyScan};

#[derive(new)]
pub struct DummyScan {
    pub(crate) plan: PhysicalDummyScan,
}

impl DummyScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let mut fields = vec![];
        for (idx, ty) in self.plan.base.types.iter().enumerate() {
            fields.push(Field::new(
                format!("col{}", idx).as_str(),
                ty.clone().into(),
                true,
            ));
        }
        let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
        yield RecordBatch::new_empty(schema.clone());
    }
}
