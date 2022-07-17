use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::*;
use crate::binder::BoundExpr;
pub struct ProjectExecutor {
    pub exprs: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl ProjectExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            let columns = self
                .exprs
                .iter()
                .map(|e| e.eval_column(&batch))
                .try_collect();
            let fields = self.exprs.iter().map(|e| e.eval_field(&batch)).collect();
            let schema = SchemaRef::new(Schema::new_with_metadata(
                fields,
                batch.schema().metadata().clone(),
            ));
            yield RecordBatch::try_new(schema, columns?)?;
        }
    }
}
