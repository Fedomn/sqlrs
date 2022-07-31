use std::collections::HashMap;

use arrow::array::UInt32Builder;
use arrow::compute;
use arrow::datatypes::{Field, Schema, SchemaRef};
use itertools::Itertools;

use super::create_accumulators;
use super::hash_utils::create_hashes;
use crate::binder::{BoundAggFunc, BoundExpr};
use crate::executor::*;
use crate::types::{append_scalar_value_for_builder, build_scalar_value_builder, ScalarValue};

pub struct HashAggExecutor {
    pub agg_funcs: Vec<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub child: BoxedExecutor,
}

impl HashAggExecutor {
    fn cast_agg_funcs(&self) -> Vec<BoundAggFunc> {
        self.agg_funcs
            .iter()
            .map(|e| match e {
                BoundExpr::AggFunc(agg) => agg.clone(),
                _ => unreachable!(""),
            })
            .collect_vec()
    }

    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let agg_funcs = self.cast_agg_funcs();

        let mut group_and_agg_fields: Option<Vec<Field>> = None;
        let mut group_hashs = Vec::new();
        let mut group_hash_2_keys = HashMap::new();
        let mut group_hash_2_accs = HashMap::new();

        #[for_await]
        for batch in self.child {
            let batch = batch?;

            // 1. build group and agg schema for hash_agg RecordBatch schema.
            if group_and_agg_fields.is_none() {
                let group_fileds = self.group_by.iter().map(|key| key.eval_field(&batch));

                let agg_fileds = agg_funcs.iter().map(|agg| {
                    let inner_name = agg.exprs[0].eval_field(&batch).name().clone();
                    let new_name = format!("{}({})", agg.func, inner_name);
                    Field::new(new_name.as_str(), agg.return_type.clone(), false)
                });

                group_and_agg_fields = Some(
                    group_fileds
                        .into_iter()
                        .chain(agg_fileds.into_iter())
                        .collect::<Vec<_>>(),
                );
            }

            // 2.1 evaluate agg exprs and collect the result columns for later accumulators.
            let columns: Vec<_> = agg_funcs
                .iter()
                .map(|agg| agg.exprs[0].eval_column(&batch))
                .try_collect()?;

            // 2.2 evaluate group by exprs and collect group by columns for row hash calculation.
            let group_keys: Vec<_> = self
                .group_by
                .iter()
                .map(|expr| expr.eval_column(&batch))
                .try_collect()?;

            // 3.1 build row hash key from group by columns.
            let mut every_rows_hashes = vec![0; batch.num_rows()];
            create_hashes(&group_keys, &Default::default(), &mut every_rows_hashes)?;

            // 3.2
            // a. build accumulator map(group_hash_2_accs) for aggregation calculation.
            // b. build group row indices map(group_hash_2_row_indices) to take one group rows
            // from a column, and use acc.update_batch to calculate the group result.
            // c. build group values map(group_hash_2_keys) to record the group values.
            // d. build group hashs vector(group_hashs) to record distinct group hash key order.
            let mut group_hash_2_row_indices = HashMap::new();
            for (row, hash) in every_rows_hashes.iter().enumerate() {
                if !group_hash_2_accs.contains_key(hash) {
                    // group key hash -> accumulator
                    group_hash_2_accs.insert(*hash, create_accumulators(&self.agg_funcs));
                    // group key hash -> row indices
                    group_hash_2_row_indices.insert(*hash, UInt32Builder::new(0));
                    // group key hash -> group keys
                    let group_by_values = group_keys
                        .iter()
                        .map(|col| ScalarValue::try_from_array(col, row))
                        .collect::<Vec<_>>();
                    group_hash_2_keys.insert(*hash, group_by_values);
                    // keep group key hash order for later result order
                    group_hashs.push(*hash);
                }

                group_hash_2_row_indices
                    .get_mut(hash)
                    .unwrap()
                    .append_value(row as u32)?;
            }

            // 4. finish aggregation result for each group.
            for (hash, mut idx_builder) in group_hash_2_row_indices {
                let indices = idx_builder.finish();
                let accs = group_hash_2_accs.get_mut(&hash).unwrap();
                for (acc, column) in accs.iter_mut().zip_eq(columns.iter()) {
                    // take one group rows from a column
                    let new_array = compute::take(column.as_ref(), &indices, None)?;
                    acc.update_batch(&new_array)?;
                }
            }
        }

        // 5.1 build result builders for hash_agg RecordBatch.
        let fields = group_and_agg_fields.unwrap();
        let mut builders = fields
            .iter()
            .map(|f| build_scalar_value_builder(f.data_type()))
            .collect::<Vec<_>>();

        // 5.2 convert row data to columnar data using builders.
        for hash in group_hashs {
            let group_values = group_hash_2_keys.get(&hash).unwrap();
            for (idx, group_key) in group_values.iter().enumerate() {
                append_scalar_value_for_builder(group_key, &mut builders[idx])?;
            }

            for (idx, acc) in group_hash_2_accs.get(&hash).unwrap().iter().enumerate() {
                append_scalar_value_for_builder(
                    &acc.evaluate()?,
                    &mut builders[idx + group_values.len()],
                )?;
            }
        }

        // 6. finish result builders and build RecordBatch.
        let columns = builders.iter_mut().map(|b| b.finish()).collect::<Vec<_>>();
        let schema = SchemaRef::new(Schema::new(fields));
        yield RecordBatch::try_new(schema, columns)?;
    }
}
