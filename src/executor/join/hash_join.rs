use std::collections::HashMap;

use arrow::array::{
    new_null_array, Array, BooleanArray, BooleanBufferBuilder, PrimitiveArray, UInt32Array,
    UInt32Builder, UInt64Array, UInt64Builder,
};
use arrow::compute;
use arrow::datatypes::{DataType, Schema, SchemaRef, UInt32Type, UInt64Type};

use crate::binder::{BoundExpr, JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;
use crate::executor::aggregate::hash_utils::create_hashes;
use crate::executor::*;

pub struct HashJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_type: JoinType,
    pub join_condition: JoinCondition,
    /// The schema once the join is applied
    pub join_output_schema: Vec<ColumnCatalog>,
}

fn build_batch(
    left_batch: &RecordBatch,
    right_batch: &RecordBatch,
    left_indices: &UInt64Array,
    right_indices: &UInt32Array,
    schema: SchemaRef,
) -> Result<RecordBatch, ExecutorError> {
    let left_array: Vec<_> = left_batch
        .columns()
        .iter()
        .map(|col| compute::take(col, left_indices, None))
        .try_collect()?;
    let right_array: Vec<_> = right_batch
        .columns()
        .iter()
        .map(|col| compute::take(col, right_indices, None))
        .try_collect()?;

    let data = vec![left_array, right_array].concat();
    Ok(RecordBatch::try_new(schema, data)?)
}

fn apply_join_filter(
    join_type: &JoinType,
    filter: &Option<BoundExpr>,
    intermediate_batch: RecordBatch,
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    right_num_rows: usize,
) -> Result<(UInt64Array, UInt32Array), ExecutorError> {
    if let Some(ref expr) = filter {
        match join_type {
            JoinType::Inner | JoinType::Left => {
                // inner and left join filter logic is same as above generate indices logic
                // so unvisited left data will handled in the last step
                let mask = expr.eval_column(&intermediate_batch)?;
                let predicate = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("join filter expected evaluate boolean array");
                let left_filter_indices = PrimitiveArray::<UInt64Type>::from(
                    compute::filter(&left_indices, predicate)?.data().clone(),
                );
                let right_filter_indices = PrimitiveArray::<UInt32Type>::from(
                    compute::filter(&right_indices, predicate)?.data().clone(),
                );
                Ok((left_filter_indices, right_filter_indices))
            }
            JoinType::Right | JoinType::Full => {
                // right and full join filter is special case. it must keep all right data
                // in the result, so in addition to filtered rows, we also need to keep
                // right side unfiltered data which is unique rows.
                let mask = expr.eval_column(&intermediate_batch)?;
                let predicate = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("join filter expected evaluate boolean array");

                let left_filter_indices = PrimitiveArray::<UInt64Type>::from(
                    compute::filter(&left_indices, predicate)?.data().clone(),
                );
                let right_filter_indices = PrimitiveArray::<UInt32Type>::from(
                    compute::filter(&right_indices, predicate)?.data().clone(),
                );
                // build right side visited row indices
                let mut visited_right_side = BooleanBufferBuilder::new(right_num_rows);
                visited_right_side.append_n(right_num_rows, false);
                right_filter_indices.iter().flatten().for_each(|x| {
                    visited_right_side.set_bit(x as usize, true);
                });
                // calculate right side unvisited row indices
                let unvisited_right_indices = UInt32Array::from_iter_values(
                    (0..visited_right_side.len())
                        .filter_map(|v| (!visited_right_side.get_bit(v)).then(|| v as u32)),
                );

                let appendnull_left_indices =
                    new_null_array(&DataType::UInt64, unvisited_right_indices.len());
                let appendnull_left_indices = appendnull_left_indices
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();

                let left = UInt64Array::from_iter(
                    left_filter_indices
                        .iter()
                        .chain(appendnull_left_indices.iter()),
                );

                let right = UInt32Array::from_iter(
                    right_filter_indices
                        .iter()
                        .chain(unvisited_right_indices.iter()),
                );

                Ok((left, right))
            }
            JoinType::Cross => unreachable!(""),
        }
    } else {
        Ok((left_indices, right_indices))
    }
}

impl HashJoinExecutor {
    fn cast_join_condition(&self) -> (Vec<(BoundExpr, BoundExpr)>, Option<BoundExpr>) {
        match &self.join_condition {
            JoinCondition::On { on, filter } => (on.clone(), filter.clone()),
            JoinCondition::None => unreachable!("HashJoin must has on condition"),
        }
    }

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
        let (on_keys, filter) = self.cast_join_condition();
        let on_left_keys = on_keys.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>();
        let on_right_keys = on_keys.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left batches into single batch.
        let hash_random_state = Default::default();
        let mut left_hashmap = HashMap::new();
        let mut left_row_offset = 0;
        let mut left_batches = vec![];
        let join_output_schema = self.join_output_arrow_schema();

        #[for_await]
        for batch in self.left_child {
            let batch = batch?;

            let left_keys: Vec<_> = on_left_keys
                .iter()
                .map(|key| key.eval_column(&batch))
                .try_collect()?;
            let mut every_rows_hashes = vec![0; batch.num_rows()];
            create_hashes(&left_keys, &hash_random_state, &mut every_rows_hashes)?;

            for (row, hash) in every_rows_hashes.iter().enumerate() {
                left_hashmap
                    .entry(*hash)
                    .or_insert_with(Vec::new)
                    .push(row + left_row_offset);
            }

            left_row_offset += batch.num_rows();
            left_batches.push(batch);
        }

        if left_batches.is_empty() {
            return Ok(());
        }

        let left_single_batch = RecordBatch::concat(&left_batches[0].schema(), &left_batches)?;

        // probe phase
        //
        // build visited_left_side to record the left data has been visited,
        // because probe phase only visit the right data, so if we use left-join or full-join,
        // the left unvisited data should be returned to meet the join semantics.
        let mut visited_left_side = match self.join_type {
            JoinType::Left | JoinType::Full => {
                let num_rows = left_single_batch.num_rows();

                let mut buffer = BooleanBufferBuilder::new(num_rows);

                buffer.append_n(num_rows, false);

                buffer
            }
            JoinType::Inner | JoinType::Right => BooleanBufferBuilder::new(0),
            JoinType::Cross => unreachable!(""),
        };
        #[for_await]
        for batch in self.right_child {
            let batch = batch?;
            let right_keys: Vec<_> = on_right_keys
                .iter()
                .map(|key| key.eval_column(&batch))
                .try_collect()?;
            let mut right_rows_hashes = vec![0; batch.num_rows()];
            create_hashes(&right_keys, &hash_random_state, &mut right_rows_hashes)?;

            // 1. build left and right indices
            let mut left_indices = UInt64Builder::new(0);
            let mut right_indices = UInt32Builder::new(0);
            match self.join_type {
                // Get the hash and find it in the build index
                // TODO: For every item on the left and right we check if it matches
                // This possibly contains rows with hash collisions,
                // So we have to check here whether rows are equal or not
                JoinType::Inner | JoinType::Left => {
                    for (row, hash) in right_rows_hashes.iter().enumerate() {
                        if let Some(indices) = left_hashmap.get(hash) {
                            for &i in indices {
                                left_indices.append_value(i as u64)?;
                                right_indices.append_value(row as u32)?;
                            }
                        }
                    }
                }
                JoinType::Right | JoinType::Full => {
                    for (row, hash) in right_rows_hashes.iter().enumerate() {
                        if let Some(indices) = left_hashmap.get(hash) {
                            for &i in indices {
                                left_indices.append_value(i as u64)?;
                                right_indices.append_value(row as u32)?;
                            }
                        } else {
                            // when no match, add the row with None for the left side
                            left_indices.append_null()?;
                            right_indices.append_value(row as u32)?;
                        }
                    }
                }
                JoinType::Cross => unreachable!("Cross join should not be in HashJoinExecutor"),
            }

            // 2. build intermediate batch that from left and right all columns
            let left_indices = left_indices.finish();
            let right_indices = right_indices.finish();

            let intermediate_batch = build_batch(
                &left_single_batch,
                &batch,
                &left_indices,
                &right_indices,
                join_output_schema.clone(),
            )?;

            // 3. apply join filter
            let (left_filter_indices, right_filter_indices) = apply_join_filter(
                &self.join_type,
                &filter,
                intermediate_batch,
                left_indices,
                right_indices,
                batch.num_rows(),
            )?;

            match self.join_type {
                JoinType::Left | JoinType::Full => {
                    left_filter_indices.iter().flatten().for_each(|x| {
                        visited_left_side.set_bit(x as usize, true);
                    });
                }
                JoinType::Right | JoinType::Inner => {}
                JoinType::Cross => unreachable!(""),
            }

            let result_batch = build_batch(
                &left_single_batch,
                &batch,
                &left_filter_indices,
                &right_filter_indices,
                join_output_schema.clone(),
            )?;
            yield result_batch;
        }

        // handle left side unvisited rows: to generate last result_batch which is consist of left
        // unvisited rows and null rows on right side.
        match self.join_type {
            JoinType::Left | JoinType::Full => {
                let indices = UInt64Array::from_iter_values(
                    (0..visited_left_side.len())
                        .filter_map(|v| (!visited_left_side.get_bit(v)).then(|| v as u64)),
                );
                let left_array: Vec<_> = left_single_batch
                    .columns()
                    .iter()
                    .map(|col| compute::take(col, &indices, None))
                    .try_collect()?;
                let offset = left_array.len();
                let right_array = join_output_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx >= offset)
                    .map(|(_, field)| {
                        arrow::array::new_null_array(field.data_type(), indices.len())
                    })
                    .collect::<Vec<_>>();
                let data = vec![left_array, right_array].concat();
                yield RecordBatch::try_new(join_output_schema.clone(), data)?;
            }
            JoinType::Right | JoinType::Inner => {}
            JoinType::Cross => unreachable!(""),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::util::pretty::pretty_format_batches;
    use futures::{StreamExt, TryStreamExt};
    use sqlparser::ast::BinaryOperator;

    use super::*;
    use crate::binder::test_util::*;
    use crate::binder::BoundBinaryOp;
    use crate::catalog::ColumnDesc;

    fn build_table_i32(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Int32, false),
            Field::new(b.0, DataType::Int32, false),
            Field::new(c.0, DataType::Int32, false),
        ]);

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap()
    }

    fn build_table_schema(
        table_id: &str,
        batch: &RecordBatch,
        nullable: bool,
    ) -> Vec<ColumnCatalog> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| ColumnCatalog {
                table_id: table_id.to_string(),
                column_id: field.name().to_string(),
                nullable,
                desc: ColumnDesc {
                    name: field.name().to_string(),
                    data_type: field.data_type().clone(),
                },
            })
            .collect()
    }

    fn build_test_child(join_type: JoinType) -> (BoxedExecutor, BoxedExecutor, Vec<ColumnCatalog>) {
        let (left_join_keys_force_nullable, right_join_keys_force_nullable) = match join_type {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => unreachable!(""),
        };

        let left_batches = vec![build_table_i32(
            ("a1", &vec![0, 1, 2, 3, 4]),
            ("b1", &vec![0, 4, 5, 5, 8]),
            ("c1", &vec![10, 7, 8, 9, 10]),
        )];
        let left_schema = build_table_schema("l", &left_batches[0], left_join_keys_force_nullable);
        let right_batches = vec![build_table_i32(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        )];
        let right_schema =
            build_table_schema("r", &right_batches[0], right_join_keys_force_nullable);

        let left_iter = left_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let left_child = futures::stream::iter(left_iter).boxed();
        let right_iter = right_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let right_child = futures::stream::iter(right_iter).boxed();

        (
            left_child,
            right_child,
            vec![left_schema, right_schema].concat(),
        )
    }

    #[tokio::test]
    async fn test_inner_join_results() {
        let (left_child, right_child, join_output_schema) = build_test_child(JoinType::Inner);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+------+------+------+------+------+------+",
            "| l.a1 | l.b1 | l.c1 | r.a2 | r.b1 | r.c2 |",
            "+------+------+------+------+------+------+",
            "| 1    | 4    | 7    | 10   | 4    | 70   |",
            "| 2    | 5    | 8    | 20   | 5    | 80   |",
            "| 3    | 5    | 9    | 20   | 5    | 80   |",
            "+------+------+------+------+------+------+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_left_join_results() {
        let (left_child, right_child, join_output_schema) = build_test_child(JoinType::Left);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Left,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+------+------+------+------+------+------+",
            "| l.a1 | l.b1 | l.c1 | r.a2 | r.b1 | r.c2 |",
            "+------+------+------+------+------+------+",
            "| 1    | 4    | 7    | 10   | 4    | 70   |",
            "| 2    | 5    | 8    | 20   | 5    | 80   |",
            "| 3    | 5    | 9    | 20   | 5    | 80   |",
            "| 0    | 0    | 10   |      |      |      |",
            "| 4    | 8    | 10   |      |      |      |",
            "+------+------+------+------+------+------+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_right_join_results() {
        let (left_child, right_child, join_output_schema) = build_test_child(JoinType::Right);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Right,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+------+------+------+------+------+------+",
            "| l.a1 | l.b1 | l.c1 | r.a2 | r.b1 | r.c2 |",
            "+------+------+------+------+------+------+",
            "| 1    | 4    | 7    | 10   | 4    | 70   |",
            "| 2    | 5    | 8    | 20   | 5    | 80   |",
            "| 3    | 5    | 9    | 20   | 5    | 80   |",
            "|      |      |      | 30   | 6    | 90   |",
            "+------+------+------+------+------+------+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_full_join_results() {
        let (left_child, right_child, join_output_schema) = build_test_child(JoinType::Full);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Full,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+------+------+------+------+------+------+",
            "| l.a1 | l.b1 | l.c1 | r.a2 | r.b1 | r.c2 |",
            "+------+------+------+------+------+------+",
            "| 1    | 4    | 7    | 10   | 4    | 70   |",
            "| 2    | 5    | 8    | 20   | 5    | 80   |",
            "| 3    | 5    | 9    | 20   | 5    | 80   |",
            "|      |      |      | 30   | 6    | 90   |",
            "| 0    | 0    | 10   |      |      |      |",
            "| 4    | 8    | 10   |      |      |      |",
            "+------+------+------+------+------+------+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    fn build_test_join_filter_child(
        join_type: JoinType,
    ) -> (BoxedExecutor, BoxedExecutor, Vec<ColumnCatalog>) {
        let (left_join_keys_force_nullable, right_join_keys_force_nullable) = match join_type {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => unreachable!(""),
        };

        let left_batches = vec![build_table_i32(
            ("a", &vec![0, 1, 2, 2]),
            ("b", &vec![4, 5, 7, 8]),
            ("c", &vec![7, 8, 9, 1]),
        )];
        let left_schema = build_table_schema("l", &left_batches[0], left_join_keys_force_nullable);
        let right_batches = vec![build_table_i32(
            ("a", &vec![10, 20, 30, 40]),
            ("b", &vec![2, 2, 3, 4]),
            ("c", &vec![7, 5, 6, 6]),
        )];
        let right_schema =
            build_table_schema("r", &right_batches[0], right_join_keys_force_nullable);

        let left_iter = left_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let left_child = futures::stream::iter(left_iter).boxed();
        let right_iter = right_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let right_child = futures::stream::iter(right_iter).boxed();

        (
            left_child,
            right_child,
            vec![left_schema, right_schema].concat(),
        )
    }

    #[tokio::test]
    async fn test_inner_join_filter_results() {
        // matched sql: select t1.*, t2.* from t1 inner join t2 on t1.a=t2.b and t1.c > t2.c;
        let (left_child, right_child, join_output_schema) =
            build_test_join_filter_child(JoinType::Inner);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(0), build_bound_input_ref(1))],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_input_ref_box(2),
                    right: build_bound_input_ref_box(5),
                    return_type: Some(DataType::Boolean),
                })),
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+",
            "| l.a | l.b | l.c | r.a | r.b | r.c |",
            "+-----+-----+-----+-----+-----+-----+",
            "| 2   | 7   | 9   | 10  | 2   | 7   |",
            "| 2   | 7   | 9   | 20  | 2   | 5   |",
            "+-----+-----+-----+-----+-----+-----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_left_join_filter_results() {
        // matched sql: select t1.*, t2.* from t1 left join t2 on t1.a=t2.b and t1.c > t2.c;
        let (left_child, right_child, join_output_schema) =
            build_test_join_filter_child(JoinType::Left);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Left,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(0), build_bound_input_ref(1))],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_input_ref_box(2),
                    right: build_bound_input_ref_box(5),
                    return_type: Some(DataType::Boolean),
                })),
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+",
            "| l.a | l.b | l.c | r.a | r.b | r.c |",
            "+-----+-----+-----+-----+-----+-----+",
            "| 2   | 7   | 9   | 10  | 2   | 7   |",
            "| 2   | 7   | 9   | 20  | 2   | 5   |",
            "| 0   | 4   | 7   |     |     |     |",
            "| 1   | 5   | 8   |     |     |     |",
            "| 2   | 8   | 1   |     |     |     |",
            "+-----+-----+-----+-----+-----+-----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_right_join_filter_results() {
        // matched sql: select t1.*, t2.* from t1 right join t2 on t1.a=t2.b and t1.c > t2.c;
        let (left_child, right_child, join_output_schema) =
            build_test_join_filter_child(JoinType::Right);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Right,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(0), build_bound_input_ref(1))],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_input_ref_box(2),
                    right: build_bound_input_ref_box(5),
                    return_type: Some(DataType::Boolean),
                })),
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+",
            "| l.a | l.b | l.c | r.a | r.b | r.c |",
            "+-----+-----+-----+-----+-----+-----+",
            "| 2   | 7   | 9   | 10  | 2   | 7   |",
            "| 2   | 7   | 9   | 20  | 2   | 5   |",
            "|     |     |     | 30  | 3   | 6   |",
            "|     |     |     | 40  | 4   | 6   |",
            "+-----+-----+-----+-----+-----+-----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_full_join_filter_results() {
        // matched sql: select t1.*, t2.* from t1 full join t2 on t1.a=t2.b and t1.c > t2.c;
        let (left_child, right_child, join_output_schema) =
            build_test_join_filter_child(JoinType::Full);

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Full,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(0), build_bound_input_ref(1))],
                filter: Some(BoundExpr::BinaryOp(BoundBinaryOp {
                    op: BinaryOperator::Gt,
                    left: build_bound_input_ref_box(2),
                    right: build_bound_input_ref_box(5),
                    return_type: Some(DataType::Boolean),
                })),
            },
            join_output_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+-----+-----+-----+-----+-----+-----+",
            "| l.a | l.b | l.c | r.a | r.b | r.c |",
            "+-----+-----+-----+-----+-----+-----+",
            "| 2   | 7   | 9   | 10  | 2   | 7   |",
            "| 2   | 7   | 9   | 20  | 2   | 5   |",
            "|     |     |     | 30  | 3   | 6   |",
            "|     |     |     | 40  | 4   | 6   |",
            "| 0   | 4   | 7   |     |     |     |",
            "| 1   | 5   | 8   |     |     |     |",
            "| 2   | 8   | 1   |     |     |     |",
            "+-----+-----+-----+-----+-----+-----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }
}
