use std::slice;

use derive_new::new;

use super::{BindError, Binder, BoundExpression};
use crate::types_v2::LogicalType;

#[derive(new)]
pub struct ExpressionBinder<'a> {
    pub(crate) binder: &'a mut Binder,
}

impl ExpressionBinder<'_> {
    pub fn bind_expression(
        &mut self,
        expr: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        match expr {
            sqlparser::ast::Expr::Identifier(ident) => {
                self.bind_column_ref_expr(slice::from_ref(ident), result_names, result_types)
            }
            sqlparser::ast::Expr::CompoundIdentifier(idents) => {
                self.bind_column_ref_expr(idents, result_names, result_types)
            }
            sqlparser::ast::Expr::BinaryOp { .. } => todo!(),
            sqlparser::ast::Expr::UnaryOp { .. } => todo!(),
            sqlparser::ast::Expr::Value(v) => {
                self.bind_constant_expr(v, result_names, result_types)
            }
            sqlparser::ast::Expr::Function(_) => todo!(),
            sqlparser::ast::Expr::Exists { .. } => todo!(),
            sqlparser::ast::Expr::Subquery(_) => todo!(),
            _ => todo!(),
        }
    }
}
