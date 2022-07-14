use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, Expr};

use super::BoundExpr;
use crate::binder::{BindError, Binder};

#[derive(Debug, Clone, PartialEq)]
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
        let left = self.bind_expr(left)?;
        let right = self.bind_expr(right)?;

        let left_return_type = match (left.return_type(), right.return_type()) {
            (None, None) => None,
            (Some(left), Some(right)) => {
                if left == right {
                    Some(left)
                } else {
                    todo!("need implict type conversion")
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
            left: Box::new(left),
            right: Box::new(right),
            return_type,
        }))
    }
}
