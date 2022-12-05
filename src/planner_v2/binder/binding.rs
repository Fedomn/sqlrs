use std::collections::HashMap;

use derive_new::new;

use super::{BindError, BoundColumnRefExpression, BoundExpressionBase, ColumnBinding};
use crate::catalog_v2::CatalogEntry;
use crate::types_v2::LogicalType;

/// A Binding represents a binding to a table, table-producing function
/// or subquery with a specified table index.
#[derive(new, Clone, Debug)]
pub struct Binding {
    /// The alias of the binding
    pub(crate) alias: String,
    /// The table index of the binding
    pub(crate) index: usize,
    pub(crate) types: Vec<LogicalType>,
    #[allow(dead_code)]
    pub(crate) names: Vec<String>,
    /// Name -> index for the names
    pub(crate) name_map: HashMap<String, usize>,
    /// The underlying catalog entry (if any)
    #[new(default)]
    pub(crate) catalog_entry: Option<CatalogEntry>,
}
impl Binding {
    pub fn has_match_binding(&self, column_name: &str) -> bool {
        self.try_get_binding_index(column_name).is_some()
    }

    pub fn try_get_binding_index(&self, column_name: &str) -> Option<usize> {
        self.name_map.get(column_name).cloned()
    }

    pub fn bind_column(
        &self,
        column_name: &str,
        depth: usize,
    ) -> Result<BoundColumnRefExpression, BindError> {
        if let Some(col_idx) = self.try_get_binding_index(column_name) {
            let col_type = self.types[col_idx].clone();
            let col_binding = ColumnBinding::new(self.index, col_idx);
            Ok(BoundColumnRefExpression::new(
                BoundExpressionBase::new(column_name.to_string(), col_type),
                col_binding,
                depth,
            ))
        } else {
            Err(BindError::Internal(format!(
                "Column {} not found in table {}",
                column_name, self.alias
            )))
        }
    }
}
