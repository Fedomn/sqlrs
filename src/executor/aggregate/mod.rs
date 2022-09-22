use arrow::array::ArrayRef;

use self::count::{CountAccumulator, DistinctCountAccumulator};
use self::min_max::{MaxAccumulator, MinAccumulator};
use self::sum::{DistinctSumAccumulator, SumAccumulator};
use super::ExecutorError;
use crate::binder::{AggFunc, BoundExpr};
use crate::types::ScalarValue;

mod count;
pub mod hash_agg;
pub mod hash_utils;
mod min_max;
pub mod simple_agg;
mod sum;

/// An accumulator represents a stateful object that lives throughout the evaluation of multiple
/// rows and generically accumulates values.
pub trait Accumulator: Send + Sync {
    /// updates the accumulator's state from a vector of arrays.
    fn update_batch(&mut self, array: &ArrayRef) -> Result<(), ExecutorError>;

    /// returns its value based on its current state.
    fn evaluate(&self) -> Result<ScalarValue, ExecutorError>;
}

fn create_accumulator(expr: &BoundExpr) -> Box<dyn Accumulator> {
    if let BoundExpr::AggFunc(agg_expr) = expr {
        match (&agg_expr.func, &agg_expr.distinct) {
            (AggFunc::Count, false) => Box::new(CountAccumulator::new()),
            (AggFunc::Count, true) => Box::new(DistinctCountAccumulator::new()),
            (AggFunc::Sum, false) => Box::new(SumAccumulator::new(agg_expr.return_type.clone())),
            (AggFunc::Sum, true) => {
                Box::new(DistinctSumAccumulator::new(agg_expr.return_type.clone()))
            }
            (AggFunc::Min, _) => Box::new(MinAccumulator::new(agg_expr.return_type.clone())),
            (AggFunc::Max, _) => Box::new(MaxAccumulator::new(agg_expr.return_type.clone())),
        }
    } else {
        unreachable!(
            "create_accumulator called with non-aggregate expression {:?}",
            expr
        );
    }
}

fn create_accumulators(exprs: &[BoundExpr]) -> Vec<Box<dyn Accumulator>> {
    exprs.iter().map(create_accumulator).collect()
}
