use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{ExecutionContext, ExecutorError, PhysicalDummyScan};
use crate::types_v2::ScalarValue;

#[derive(new)]
pub struct DummyScan {
    pub(crate) _plan: PhysicalDummyScan,
}

impl DummyScan {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let fields = vec![Field::new("dummy", DataType::Boolean, true)];
        let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
        let array = ScalarValue::Boolean(Some(true)).to_array();
        yield RecordBatch::try_new(schema.clone(), vec![array])?;
    }
}
