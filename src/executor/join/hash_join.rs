use std::collections::HashMap;

use arrow::array::{BooleanBufferBuilder, UInt32Builder, UInt64Array, UInt64Builder};
use arrow::compute;
use arrow::datatypes::{Field, Schema, SchemaRef};

use crate::binder::{BoundExpr, JoinCondition, JoinType};
use crate::catalog::ColumnCatalog;
use crate::executor::aggregate::hash_utils::create_hashes;
use crate::executor::*;

pub struct HashJoinExecutor {
    pub left_child: BoxedExecutor,
    pub right_child: BoxedExecutor,
    pub join_type: JoinType,
    pub join_condition: JoinCondition,
    /// left child output schema
    pub left_schema: Vec<ColumnCatalog>,
    /// right child output schema
    pub right_schema: Vec<ColumnCatalog>,
}

impl HashJoinExecutor {
    fn cast_join_condition(&self) -> (Vec<(BoundExpr, BoundExpr)>, Option<BoundExpr>) {
        match &self.join_condition {
            JoinCondition::On { on, filter } => (on.clone(), filter.clone()),
            JoinCondition::None => unreachable!("HashJoin must has on condition"),
        }
    }

    fn left_batch_schema(&self) -> SchemaRef {
        let fields = self
            .left_schema
            .iter()
            .map(|c| c.to_arrow_field())
            .collect::<Vec<_>>();
        SchemaRef::new(Schema::new(fields))
    }

    fn merged_join_batch_schema(&self) -> SchemaRef {
        let (left_join_keys_is_nullable, right_join_keys_is_nullable) = match self.join_type {
            JoinType::Inner => (false, false),
            JoinType::Left => (false, true),
            JoinType::Right => (true, false),
            JoinType::Full => (true, true),
            JoinType::Cross => unreachable!(""),
        };
        let left_fields = self
            .left_schema
            .iter()
            .map(|c| {
                Field::new(
                    c.column_id.clone().as_str(),
                    c.desc.data_type.clone(),
                    // to handle some original left fields that are nullable
                    left_join_keys_is_nullable || c.nullable,
                )
            })
            .collect::<Vec<_>>();
        let right_fields = self
            .right_schema
            .iter()
            .map(|c| {
                Field::new(
                    c.column_id.clone().as_str(),
                    c.desc.data_type.clone(),
                    right_join_keys_is_nullable || c.nullable,
                )
            })
            .collect::<Vec<_>>();
        SchemaRef::new(Schema::new(vec![left_fields, right_fields].concat()))
    }

    #[try_stream(boxed, ok = RecordBatch, error = ExecutorError)]
    pub async fn execute(self) {
        let (on_keys, _) = self.cast_join_condition();
        let on_left_keys = on_keys.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>();
        let on_right_keys = on_keys.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

        // build phase:
        // 1.construct hashtable, one hash key may contains multiple rows indices.
        // 2.merged all left batches into single batch.
        let hash_random_state = Default::default();
        let mut left_hashmap = HashMap::new();
        let mut left_row_offset = 0;
        let mut left_batches = vec![];
        let left_batch_schema = self.left_batch_schema();
        let merged_schema = self.merged_join_batch_schema();

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

        let left_single_batch = RecordBatch::concat(&left_batch_schema, &left_batches)?;

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
            // TODO: 2. apply join filter
            // 3. build result batch that from left and right all columns
            let left_indices = left_indices.finish();
            let right_indices = right_indices.finish();
            let left_array: Vec<_> = left_single_batch
                .columns()
                .iter()
                .map(|col| compute::take(col, &left_indices, None))
                .try_collect()?;
            let right_array: Vec<_> = batch
                .columns()
                .iter()
                .map(|col| compute::take(col, &right_indices, None))
                .try_collect()?;

            match self.join_type {
                JoinType::Left | JoinType::Full => {
                    left_indices.iter().flatten().for_each(|x| {
                        visited_left_side.set_bit(x as usize, true);
                    });
                }
                JoinType::Right | JoinType::Inner => {}
                JoinType::Cross => unreachable!(""),
            }

            yield RecordBatch::try_new(
                merged_schema.clone(),
                vec![left_array, right_array].concat(),
            )?;
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
                let right_array = merged_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| *idx >= offset)
                    .map(|(_, field)| {
                        arrow::array::new_null_array(field.data_type(), indices.len())
                    })
                    .collect::<Vec<_>>();
                yield RecordBatch::try_new(
                    merged_schema.clone(),
                    vec![left_array, right_array].concat(),
                )?;
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

    use super::*;
    use crate::binder::test_util::*;

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

    fn build_table_schema(table_id: &str, batch: &RecordBatch) -> Vec<ColumnCatalog> {
        batch
            .schema()
            .fields()
            .iter()
            .map(|field| ColumnCatalog::from_arrow_field(table_id, field))
            .collect()
    }

    fn build_test_child() -> (
        BoxedExecutor,
        Vec<ColumnCatalog>,
        BoxedExecutor,
        Vec<ColumnCatalog>,
    ) {
        let left_batches = vec![build_table_i32(
            ("a1", &vec![0, 1, 2, 3, 4]),
            ("b1", &vec![0, 4, 5, 5, 8]),
            ("c1", &vec![10, 7, 8, 9, 10]),
        )];
        let left_schema = build_table_schema("l", &left_batches[0]);
        let right_batches = vec![build_table_i32(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        )];
        let right_schema = build_table_schema("r", &right_batches[0]);
        let left_iter = left_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let left_child = futures::stream::iter(left_iter).boxed();
        let right_iter = right_batches
            .into_iter()
            .map(|b| -> Result<RecordBatch, ExecutorError> { Ok(b) });
        let right_child = futures::stream::iter(right_iter).boxed();

        (left_child, left_schema, right_child, right_schema)
    }

    #[tokio::test]
    async fn test_inner_join_results() {
        let (left_child, left_schema, right_child, right_schema) = build_test_child();

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Inner,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            left_schema,
            right_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_left_join_results() {
        let (left_child, left_schema, right_child, right_schema) = build_test_child();

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Left,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            left_schema,
            right_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "| 0  | 0  | 10 |    |    |    |",
            "| 4  | 8  | 10 |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_right_join_results() {
        let (left_child, left_schema, right_child, right_schema) = build_test_child();

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Right,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            left_schema,
            right_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }

    #[tokio::test]
    async fn test_full_join_results() {
        let (left_child, left_schema, right_child, right_schema) = build_test_child();

        let executor = HashJoinExecutor {
            left_child,
            right_child,
            join_type: JoinType::Full,
            join_condition: JoinCondition::On {
                on: vec![(build_bound_input_ref(1), build_bound_input_ref(1))],
                filter: None,
            },
            left_schema,
            right_schema,
        };

        let output = executor.execute().try_collect::<Vec<_>>().await.unwrap();
        let table = pretty_format_batches(&output).unwrap().to_string();
        let actual: Vec<&str> = table.lines().collect();

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "|    |    |    | 30 | 6  | 90 |",
            "| 0  | 0  | 10 |    |    |    |",
            "| 4  | 8  | 10 |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(expected, actual, "Actual result:\n{}", table);
    }
}
