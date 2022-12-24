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
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                self.bind_binary_op_internal(left, op, right, result_names, result_types)
            }
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

    fn bind_binary_op_internal(
        &mut self,
        left: &sqlparser::ast::Expr,
        op: &sqlparser::ast::BinaryOperator,
        right: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        match op {
            sqlparser::ast::BinaryOperator::Plus
            | sqlparser::ast::BinaryOperator::Minus
            | sqlparser::ast::BinaryOperator::Multiply
            | sqlparser::ast::BinaryOperator::Divide => {
                self.bind_function_expression(left, op, right, result_names, result_types)
            }
            sqlparser::ast::BinaryOperator::Gt
            | sqlparser::ast::BinaryOperator::Lt
            | sqlparser::ast::BinaryOperator::GtEq
            | sqlparser::ast::BinaryOperator::LtEq
            | sqlparser::ast::BinaryOperator::Eq
            | sqlparser::ast::BinaryOperator::NotEq => {
                self.bind_comparison_expression(left, op, right, result_names, result_types)
            }
            sqlparser::ast::BinaryOperator::And => todo!(),
            sqlparser::ast::BinaryOperator::Or => todo!(),
            other => Err(BindError::UnsupportedExpr(other.to_string())),
        }
    }
}
