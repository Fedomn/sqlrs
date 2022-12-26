use std::collections::HashMap;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::ExecutorError;
use crate::planner_v2::BoundExpression;
use crate::types_v2::{LogicalType, ScalarValue};

pub struct SchemaUtil;

impl SchemaUtil {
    pub fn new_schema_ref(names: &[String], types: &[LogicalType]) -> SchemaRef {
        let fields = names
            .iter()
            .zip(types.iter())
            .map(|(name, ty)| Field::new(name, ty.clone().into(), true))
            .collect::<Vec<_>>();
        SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()))
    }

    pub fn new_schema_ref_from_exprs(exprs: &[BoundExpression]) -> SchemaRef {
        let fields = exprs
            .iter()
            .map(|e| Field::new(&e.alias(), e.return_type().into(), true))
            .collect::<Vec<_>>();
        SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()))
    }
}

pub struct RecordBatchUtil;

impl RecordBatchUtil {
    pub fn new_one_row_dummy_batch() -> Result<RecordBatch, ExecutorError> {
        let fields = vec![Field::new("dummy", DataType::Boolean, true)];
        let schema = SchemaRef::new(Schema::new_with_metadata(fields, HashMap::new()));
        let array = ScalarValue::Boolean(Some(true)).to_array();
        let batch = RecordBatch::try_new(schema, vec![array])?;
        Ok(batch)
    }
}
