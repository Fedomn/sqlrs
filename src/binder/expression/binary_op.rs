use std::fmt;

use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, Expr};

use super::BoundExpr;
use crate::binder::{BindError, Binder, BoundTypeCast};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BoundBinaryOp {
    pub op: BinaryOperator,
    pub left: Box<BoundExpr>,
    pub right: Box<BoundExpr>,
    pub return_type: Option<DataType>,
}

impl Binder {
    pub fn bind_binary_op(
        &mut self,
        left: &Expr,
        op: &BinaryOperator,
        right: &Expr,
    ) -> Result<BoundExpr, BindError> {
        let mut left_expr = self.bind_expr(left)?;
        let mut right_expr = self.bind_expr(right)?;

        let left_return_type = match (left_expr.return_type(), right_expr.return_type()) {
            (None, None) => None,
            (Some(left_type), Some(right_type)) => {
                if left_type == right_type {
                    Some(left_type)
                } else {
                    let mut return_type = left_type.clone();
                    match (left_type.clone(), right_type.clone()) {
                        // big type to small type, cast right to big type
                        (DataType::Int64, DataType::Int32)
                        | (DataType::Float64, DataType::Int32 | DataType::Int64) => {
                            right_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(right_expr),
                                cast_type: left_type,
                            });
                        }
                        // small type to big type, cast left to big type
                        (DataType::Int32, DataType::Int64)
                        | (DataType::Int32 | DataType::Int64, DataType::Float64) => {
                            left_expr = BoundExpr::TypeCast(BoundTypeCast {
                                expr: Box::new(left_expr),
                                cast_type: right_type.clone(),
                            });
                            return_type = right_type;
                        }
                        _ => todo!("not implmented type conversion"),
                    }
                    Some(return_type)
                }
            }
            (left, right) => {
                return Err(BindError::BinaryOpTypeMismatch(
                    format!("{:?}", left),
                    format!("{:?}", right),
                ))
            }
        };

        use BinaryOperator as Op;

        let return_type = match op {
            Op::Plus | Op::Minus | Op::Multiply | Op::Divide | Op::Modulo => left_return_type,
            Op::Gt | Op::GtEq | Op::Lt | Op::LtEq | Op::Eq | Op::NotEq | Op::And | Op::Or => {
                Some(DataType::Boolean)
            }
            o => todo!("not supported binary operator: {:?}", o),
        };
        Ok(BoundExpr::BinaryOp(BoundBinaryOp {
            op: op.clone(),
            left: Box::new(left_expr),
            right: Box::new(right_expr),
            return_type,
        }))
    }
}

impl fmt::Debug for BoundBinaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?} {} {:?}", self.left, self.op, self.right)
    }
}
