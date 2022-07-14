use arrow::array::ArrayRef;
use arrow::datatypes::Field;
use arrow::record_batch::RecordBatch;

use crate::binder::BoundExpr;

impl BoundExpr {
    /// evaluate the bound expr on the given record batch.
    pub fn eval_column(&self, batch: &RecordBatch) -> ArrayRef {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.column(input_ref.index).clone(),
            _ => unimplemented!("expr type {:?} not implemented yet", self),
        }
    }

    pub fn eval_field(&self, batch: &RecordBatch) -> Field {
        match &self {
            BoundExpr::InputRef(input_ref) => batch.schema().field(input_ref.index).clone(),
            _ => unimplemented!("expr type {:?} not implemented yet", self),
        }
    }
}
