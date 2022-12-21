use derive_new::new;
use itertools::Itertools;
use log::debug;

use super::{BoundExpression, BoundExpressionBase, ColumnBinding};
use crate::planner_v2::{BindError, ExpressionBinder, LOGGING_TARGET};
use crate::types_v2::LogicalType;

/// A BoundColumnRef expression represents a ColumnRef expression that was bound to an actual table
/// and column index. It is not yet executable, however. The ColumnBindingResolver transforms the
/// BoundColumnRefExpressions into BoundReferenceExpressions, which refer to indexes into the
/// physical chunks that pass through the executor.
#[derive(new, Debug, Clone)]
pub struct BoundColumnRefExpression {
    pub(crate) base: BoundExpressionBase,
    /// Column index set by the binder, used to generate the final BoundReferenceExpression
    pub(crate) binding: ColumnBinding,
    /// The subquery depth (i.e. depth 0 = current query, depth 1 = parent query, depth 2 = parent
    /// of parent, etc...). This is only non-zero for correlated expressions inside subqueries.
    pub(crate) depth: usize,
}

impl ExpressionBinder<'_> {
    /// qualify column name with existing table name
    fn qualify_column_name(
        &self,
        table_name: Option<&String>,
        column_name: &String,
    ) -> Result<(String, String), BindError> {
        if let Some(table_name) = table_name {
            Ok((table_name.to_string(), column_name.to_string()))
        } else {
            let table_name = self.binder.bind_context.get_matching_binding(column_name)?;
            Ok((table_name, column_name.to_string()))
        }
    }

    pub fn bind_column_ref_expr(
        &mut self,
        idents: &[sqlparser::ast::Ident],
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        let idents = idents
            .iter()
            .map(|ident| ident.value.to_lowercase())
            .collect_vec();

        let (_schema_name, table_name, column_name) = match idents.as_slice() {
            [column] => (None, None, column),
            [table, column] => (None, Some(table), column),
            [schema, table, column] => (Some(schema), Some(table), column),
            _ => return Err(BindError::UnsupportedExpr(format!("{:?}", idents))),
        };

        let (table_name, column_name) = self.qualify_column_name(table_name, column_name)?;

        // check table_name, and column_name
        if self.binder.has_match_binding(&table_name, &column_name) {
            let bound_col_ref = self
                .binder
                .bind_context
                .bind_column(&table_name, &column_name)?;
            result_names.push(bound_col_ref.base.alias.clone());
            result_types.push(bound_col_ref.base.return_type.clone());
            Ok(BoundExpression::BoundColumnRefExpression(bound_col_ref))
        } else {
            debug!(
                target: LOGGING_TARGET,
                "Planner binder context: {:#?}", self.binder.bind_context
            );
            Err(BindError::Internal(format!(
                "column not found: {}",
                column_name
            )))
        }
    }
}
