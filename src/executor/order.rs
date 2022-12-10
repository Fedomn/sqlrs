use arrow::compute::{
    concat_batches, lexsort_to_indices, take, SortColumn, SortOptions, TakeOptions,
};

use super::*;
use crate::binder::BoundOrderBy;

pub struct OrderExecutor {
    pub order_by: Vec<BoundOrderBy>,
    pub child: BoxedExecutor,
}

impl OrderExecutor {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let mut schema = None;
        let mut batches = vec![];
        #[for_await]
        for batch in self.child {
            let batch = batch?;
            if schema.is_none() {
                schema = Some(batch.schema());
            }
            batches.push(batch);
        }

        let schema = schema.unwrap();
        let batch = concat_batches(&schema, &batches)?;

        let sort_columns = self
            .order_by
            .iter()
            .map(|expr| -> Result<SortColumn, ExecutorError> {
                let sort_array = expr.expr.eval_column(&batch)?;
                Ok(SortColumn {
                    values: sort_array,
                    options: Some(SortOptions {
                        descending: !expr.asc,
                        ..Default::default()
                    }),
                })
            })
            .try_collect::<Vec<_>>()?;

        let indices = lexsort_to_indices(&sort_columns, None)?;

        let sorted_batch = RecordBatch::try_new(
            schema,
            batch
                .columns()
                .iter()
                .map(|column| {
                    take(
                        column.as_ref(),
                        &indices,
                        // disable bound check overhead since indices are already generated from
                        // the same record batch
                        Some(TakeOptions {
                            check_bounds: false,
                        }),
                    )
                })
                .try_collect::<Vec<_>>()?,
        )?;

        yield sorted_batch;
    }
}
