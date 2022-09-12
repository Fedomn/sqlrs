use crate::catalog::ColumnCatalog;

/// Return true when left is subset of right, only compare table_id and column_id, so it's safe to
/// used for join output cols with nullable columns.
pub fn is_subset_cols(left: &[ColumnCatalog], right: &[ColumnCatalog]) -> bool {
    left.iter().all(|l| right.contains(l))
}
