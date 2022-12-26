use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use derive_new::new;
use futures_async_stream::try_stream;

use crate::execution::{BoxedExecutor, ExecutionContext, ExecutorError, PhysicalLimit};

#[derive(new)]
pub struct Limit {
    pub(crate) plan: PhysicalLimit,
    pub(crate) child: BoxedExecutor,
}

impl Limit {
    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self, _context: Arc<ExecutionContext>) {
        let limit = self.plan.limit;

        let offset_val = self.plan.offset.unwrap_or(0);

        if limit.is_some() && limit.unwrap() == 0 {
            return Ok(());
        }

        let mut returned_count = 0;

        #[for_await]
        for batch in self.child {
            let batch = batch?;

            let cardinality = batch.num_rows() as u64;
            let limit_val = limit.unwrap_or(cardinality);

            let start = returned_count.max(offset_val) - returned_count;
            let end = {
                // from total returned rows level, the total_end is end index of whole returned
                // rows level.
                let total_end = offset_val + limit_val;
                let current_batch_end = returned_count + cardinality;
                // we choose the min of total_end and current_batch_end as the end index of to
                // match limit semantics.
                let real_end = total_end.min(current_batch_end);
                // to calculate the end index of current batch
                real_end - returned_count
            };

            returned_count += cardinality;

            // example: offset=1000, limit=2, cardinality=100
            // when first loop:
            // start = 0.max(1000)-0 = 1000
            // end = (1000+2).min(0+100)-0 = 100
            // so, start(1000) > end(100), we skip this loop batch.
            if start >= end {
                continue;
            }

            if (start..end) == (0..cardinality) {
                yield batch;
            } else {
                let length = end - start;
                yield batch.slice(start as usize, length as usize);
            }

            // dut to returned_count is always += cardinality, and returned_batch maybe slsliced,
            // so it will larger than real total_end.
            // example: offset=1, limit=4, cardinality=6, data=[(0..6)]
            // returned_count=6 > 1+4, meanwhile returned_batch size is 4 ([0..5])
            if returned_count >= offset_val + limit_val {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Range;
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{StreamExt, TryStreamExt};
    use itertools::Itertools;
    use test_case::test_case;

    use super::*;
    use crate::execution::PhysicalOperatorBase;
    use crate::main_entry::{ClientContext, DatabaseInstance};

    #[test_case(&[(0..6)], 1, 4, &[(1..5)])]
    #[test_case(&[(0..6)], 0, 10, &[(0..6)])]
    #[test_case(&[(0..6)], 10, 0, &[])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 4, &[(1..2),(2..4),(4..5)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 1, 2, &[(1..2),(2..3)])]
    #[test_case(&[(0..2), (2..4), (4..6)], 3, 0, &[])]
    #[tokio::test]
    async fn limit(
        inputs: &'static [Range<i32>],
        offset: u64,
        limit: u64,
        outputs: &'static [Range<i32>],
    ) {
        let executor = Limit {
            plan: PhysicalLimit::new(PhysicalOperatorBase::default(), Some(limit), Some(offset)),
            child: futures::stream::iter(inputs.iter().map(range_to_chunk).map(Ok)).boxed(),
        };
        let ctx = Arc::new(ExecutionContext::new(ClientContext::new(Arc::new(
            DatabaseInstance::default(),
        ))));
        let actual = executor.execute(ctx).try_collect::<Vec<_>>().await.unwrap();
        let outputs = outputs.iter().map(range_to_chunk).collect_vec();
        assert_eq!(actual, outputs);
    }

    fn range_to_chunk(range: &Range<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let data: Vec<_> = range.clone().collect();
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(data))]).unwrap()
    }
}
