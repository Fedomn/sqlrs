use arrow::array::ArrayRef;
use arrow::compute::cast;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;

use super::*;
use crate::binder::BoundExpr;
use crate::types::build_scalar_value_array;

/// Evaluate the bound expr on the given record batch.
/// The core computation logic directly used arrow compute kernels in arrow::compute::kernels.
impl BoundExpr {
    pub fn eval_column(&self, batch: &RecordBatch) -> Result<ArrayRef, ExecutorError> {
        match &self {
            BoundExpr::InputRef(input_ref) => Ok(batch.column(input_ref.index).clone()),
            BoundExpr::BinaryOp(expr) => {
                let left = expr.left.eval_column(batch)?;
                let right = expr.right.eval_column(batch)?;
                binary_op(&left, &right, &expr.op)
            }
            BoundExpr::Constant(val) => Ok(build_scalar_value_array(val, batch.num_rows())),
            BoundExpr::ColumnRef(_) => panic!("column ref should be resolved"),
            BoundExpr::TypeCast(tc) => Ok(cast(&tc.expr.eval_column(batch)?, &tc.cast_type)?),
            BoundExpr::AggFunc(_) => todo!(),
            BoundExpr::Alias(alias) => alias.expr.eval_column(batch),
        }
    }

    pub fn eval_field(&self, batch: &RecordBatch) -> Field {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.schema().field(input_ref.index).clone(),
            BoundExpr::BinaryOp(expr) => {
                let left = expr.left.eval_field(batch);
                let right = expr.right.eval_field(batch);
                let new_name = format!("{}{}{}", left.name(), expr.op, right.name());
                let data_type = expr.return_type.clone().unwrap();
                Field::new(new_name.as_str(), data_type, true)
            }
            BoundExpr::Constant(val) => {
                Field::new(format!("{}", val).as_str(), val.data_type(), true)
            }
            BoundExpr::TypeCast(tc) => {
                let inner_field = tc.expr.eval_field(batch);
                let new_name = format!("{}({})", tc.cast_type, inner_field.name());
                Field::new(new_name.as_str(), tc.cast_type.clone(), true)
            }
            BoundExpr::ColumnRef(col) => {
                let col_desc = col.column_catalog.desc.clone();
                Field::new(&col_desc.name, col_desc.data_type, true)
            }
            BoundExpr::AggFunc(agg) => {
                let inner_name = agg.exprs[0].eval_field(batch).name().clone();
                let new_name = format!("{}({})", agg.func, inner_name);
                Field::new(new_name.as_str(), agg.return_type.clone(), true)
            }
            BoundExpr::Alias(alias) => {
                let new_name = alias.column_id.to_string();
                let data_type = alias.expr.return_type().unwrap();
                Field::new(new_name.as_str(), data_type, true)
            }
        }
    }
}

#[cfg(test)]
mod evaluator_test {
    use std::sync::Arc;

    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    use crate::binder::{BoundExpr, BoundInputRef, BoundTypeCast};
    use crate::executor::ExecutorError;

    fn build_record_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![3, 4])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_eval_column_for_input_ref() -> Result<(), ExecutorError> {
        let batch = build_record_batch();
        let expr = BoundExpr::InputRef(BoundInputRef {
            index: 1,
            return_type: DataType::Int32,
        });
        let result = expr.eval_column(&batch)?;
        assert_eq!(result.len(), 2);
        assert_eq!(*result, Int32Array::from(vec![3, 4]));
        Ok(())
    }

    #[test]
    fn test_eval_column_for_type_cast() -> Result<(), ExecutorError> {
        let batch = build_record_batch();
        let expr = BoundExpr::TypeCast(BoundTypeCast {
            expr: Box::new(BoundExpr::InputRef(BoundInputRef {
                index: 1,
                return_type: DataType::Int32,
            })),
            cast_type: DataType::Int64,
        });
        let result = expr.eval_column(&batch)?;
        assert_eq!(result.len(), 2);
        assert_eq!(*result, Int64Array::from(vec![3, 4]));
        Ok(())
    }
}
