pub mod tree_render;

use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use arrow::util::pretty::print_batches;

use crate::optimizer::PlanNode;
use crate::types_v2::LogicalType;

pub fn pretty_batches_with(batches: &[RecordBatch], names: &[String], types: &[LogicalType]) {
    let fields = names
        .iter()
        .zip(types.iter())
        .map(|(name, data_type)| Field::new(name.as_str(), data_type.clone().into(), true))
        .collect::<Vec<_>>();
    let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
    let batches = batches
        .iter()
        .map(|batch| RecordBatch::try_new(schema.clone(), batch.columns().to_vec()))
        .collect::<Result<Vec<_>, ArrowError>>()
        .unwrap();
    _ = print_batches(batches.as_slice());
}

pub fn pretty_batches(batches: &Vec<RecordBatch>) {
    _ = print_batches(batches.as_slice());
}

pub fn pretty_plan_tree_string(plan: &dyn PlanNode) -> String {
    let mut explain_result = String::new();
    plan.explain(0, &mut explain_result);
    explain_result
}

pub fn pretty_plan_tree(plan: &dyn PlanNode) {
    let mut explain_result = String::new();
    plan.explain(0, &mut explain_result);
    println!("{}", explain_result);
}

/// follow rules: https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
/// NULL values are rendered as "NULL".
/// Empty strings are rendered as "(empty)".
///
/// ```markdown
/// SLT pattern: query <type-string> <sort-mode> <label>
/// - <type-string>: "T" for a text result, "I" for an integer result, and "R" for a floating-point result.
/// ```
pub fn record_batch_to_string(batch: &RecordBatch) -> Result<String, ArrowError> {
    let mut output = String::new();
    for row in 0..batch.num_rows() {
        for col in 0..batch.num_columns() {
            if col != 0 {
                output.push(' ');
            }
            let column = batch.column(col);

            // NULL values are rendered as "NULL".
            if column.is_null(row) {
                output.push_str("NULL");
                continue;
            }
            let string = array_value_to_string(column, row)?;

            // Empty strings are rendered as "(empty)".
            if *column.data_type() == DataType::Utf8 && string.is_empty() {
                output.push_str("(empty)");
                continue;
            }
            output.push_str(&string);
        }
        output.push('\n');
    }

    Ok(output)
}

#[cfg(test)]
mod util_test {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::error::ArrowError;
    use arrow::record_batch::RecordBatch;

    use crate::util::record_batch_to_string;

    fn build_record_batch() -> Result<RecordBatch, ArrowError> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("first_name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Bill", "Gregg", "John"])),
            ],
        )?;
        Ok(batch)
    }

    #[test]
    fn test_record_batch_to_string() -> Result<(), ArrowError> {
        let record_batch = build_record_batch()?;
        let output = record_batch_to_string(&record_batch)?;

        let expected = vec!["1 Bill", "2 Gregg", "3 John"];
        let actual: Vec<&str> = output.lines().collect();
        assert_eq!(expected, actual);

        Ok(())
    }
}
