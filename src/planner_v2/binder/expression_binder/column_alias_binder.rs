use std::collections::HashMap;

use derive_new::new;
use expression_binder::ExpressionBinder;

use crate::planner_v2::{expression_binder, BindError, BoundExpression};

/// A helper binder for WhereBinder and HavingBinder which support alias as a columnref.
#[derive(new)]
pub struct ColumnAliasBinder<'a> {
    pub(crate) original_select_items: &'a [sqlparser::ast::Expr],
    pub(crate) alias_map: &'a HashMap<String, usize>,
}

impl<'a> ColumnAliasBinder<'a> {
    pub fn bind_alias(
        &self,
        expression_binder: &mut ExpressionBinder,
        expr: &sqlparser::ast::Expr,
    ) -> Result<BoundExpression, BindError> {
        if let sqlparser::ast::Expr::Identifier(ident) = expr {
            let alias = ident.to_string();
            if let Some(alias_entry) = self.alias_map.get(&alias) {
                let expr = self.original_select_items[*alias_entry].clone();
                let bound_expr =
                    expression_binder.bind_expression(&expr, &mut vec![], &mut vec![])?;
                return Ok(bound_expr);
            }
        }
        Err(BindError::Internal(format!("column not found: {}", expr)))
    }
}
