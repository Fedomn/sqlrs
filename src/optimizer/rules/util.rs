use arrow::datatypes::DataType;
use sqlparser::ast::BinaryOperator;

use crate::binder::{BoundBinaryOp, BoundExpr};
use crate::catalog::ColumnCatalog;

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
/// If left equals right, return true.
pub fn is_subset_cols(left: &[ColumnCatalog], right: &[ColumnCatalog]) -> bool {
    left.iter().all(|l| right.contains(l))
}

/// Return true when left is superset of right.
/// If left equals right, return false.
pub fn is_superset_cols(left: &[ColumnCatalog], right: &[ColumnCatalog]) -> bool {
    right.iter().all(|r| left.contains(r)) && left.len() > right.len()
}

/// Return true when left is subset of right
pub fn is_subset_exprs(left: &[BoundExpr], right: &[BoundExpr]) -> bool {
    left.iter().all(|l| right.contains(l))
}

/// Reduce multi predicates into a conjunctive predicate by AND
pub fn reduce_conjunctive_predicate(exprs: Vec<BoundExpr>) -> Option<BoundExpr> {
    exprs.into_iter().reduce(|a, b| {
        BoundExpr::BinaryOp(BoundBinaryOp {
            op: BinaryOperator::And,
            left: Box::new(a),
            right: Box::new(b),
            return_type: Some(DataType::Boolean),
        })
    })
}
