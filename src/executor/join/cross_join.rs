use arrow::compute::concat_batches;
use arrow::datatypes::{Schema, SchemaRef};

use crate::catalog::ColumnCatalog;
use crate::executor::*;
use crate::types::{build_scalar_value_array, ScalarValue};

pub struct CrossJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    /// The schema once the join is applied
    pub join_output_schema: Vec<ColumnCatalog>,
}

impl CrossJoinExecutor {
    fn join_output_arrow_schema(&self) -> SchemaRef {
        let fields = self
            .join_output_schema
            .iter()
            .map(|c| c.to_arrow_field())
            .collect::<Vec<_>>();
        SchemaRef::new(Schema::new(fields))
    }

    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let schema = self.join_output_arrow_schema();

        // consume all left stream data and then iterate right stream chunk to build result
        let left_batches = self.left_child.try_collect::<Vec<_>>().await?;

        if left_batches.is_empty() {
            return Ok(());
        }

        let left_single_batch = concat_batches(&left_batches[0].schema(), &left_batches)?;

        #[for_await]
        for right_batch in self.right_child {
            let right_data = right_batch?;

            // repeat left value n times to match right batch size
            for row_idx in 0..left_single_batch.num_rows() {
                let new_left_data = left_single_batch
                    .columns()
                    .iter()
                    .map(|col_arr| {
                        let scalar = ScalarValue::try_from_array(col_arr, row_idx);
                        build_scalar_value_array(&scalar, right_data.num_rows())
                    })
                    .collect::<Vec<_>>();
                // concat left and right data
                let data = vec![new_left_data, right_data.columns().to_vec()].concat();
                yield RecordBatch::try_new(schema.clone(), data)?
            }
        }
    }
}
