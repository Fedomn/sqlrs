use derive_new::new;

use super::ColumnAliasBinder;
use crate::planner_v2::{BindError, BoundExpression, ExpressionBinder};
use crate::types_v2::LogicalType;

/// The WHERE binder is responsible for binding an expression within the WHERE clause of a SQL
/// statement
#[derive(new)]
pub struct WhereBinder<'a> {
    internal_binder: ExpressionBinder<'a>,
    column_alias_binder: ColumnAliasBinder<'a>,
}

impl<'a> WhereBinder<'a> {
    pub fn bind_expression(
        &mut self,
        expr: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        match expr {
            sqlparser::ast::Expr::Identifier(..) | sqlparser::ast::Expr::CompoundIdentifier(..) => {
                self.bind_column_ref_expr(expr, result_names, result_types)
            }
            other => self
                .internal_binder
                .bind_expression(other, result_names, result_types),
        }
    }

    fn bind_column_ref_expr(
        &mut self,
        expr: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        // bind  column ref expr first
        let bind_res = self
            .internal_binder
            .bind_expression(expr, result_names, result_types);
        if bind_res.is_ok() {
            return bind_res;
        }
        // try to bind as alias
        self.column_alias_binder
            .bind_alias(&mut self.internal_binder, expr)
    }
}
