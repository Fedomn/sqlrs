use derive_new::new;
use sqlparser::ast::{Expr, Query};

use crate::execution::ExpressionExecutor;
use crate::planner_v2::{
    BindError, Binder, BoundCastExpression, BoundExpression, ExpressionBinder,
};
use crate::types_v2::{LogicalType, ScalarValue};

#[derive(Debug)]
pub enum BoundResultModifier {
    BoundLimitModifier(BoundLimitModifier),
}

#[derive(new, Debug)]
pub struct BoundLimitModifier {
    pub(crate) limit_value: u64,
    pub(crate) offsert_value: u64,
    pub(crate) limit: Option<BoundExpression>,
    pub(crate) offset: Option<BoundExpression>,
}

impl Binder {
    fn bind_delimiter(
        expr_binder: &mut ExpressionBinder,
        expr: &Expr,
    ) -> Result<BoundExpression, BindError> {
        let bound_expr = expr_binder.bind_expression(expr, &mut vec![], &mut vec![])?;
        let new_expr =
            BoundCastExpression::try_add_cast_to_type(bound_expr, LogicalType::UBigint, false)?;
        Ok(new_expr)
    }

    fn cast_delimiter_val(val: ScalarValue) -> u64 {
        match val {
            ScalarValue::UInt64(Some(v)) => v,
            _ => unreachable!("delimiter val must be int64 due to previous cast"),
        }
    }

    pub fn bind_limit_modifier(
        &mut self,
        query: &Query,
    ) -> Result<Option<BoundResultModifier>, BindError> {
        let mut expr_binder = ExpressionBinder::new(self);
        let limit = query
            .limit
            .as_ref()
            .map(|expr| Self::bind_delimiter(&mut expr_binder, expr))
            .transpose()?;
        let limit_value = if let Some(limit_expr) = &limit {
            let val = ExpressionExecutor::execute_scalar(limit_expr)?;
            Self::cast_delimiter_val(val)
        } else {
            u64::max_value()
        };

        let offset = query
            .offset
            .as_ref()
            .map(|expr| Self::bind_delimiter(&mut expr_binder, &expr.value))
            .transpose()?;
        let offsert_value = if let Some(offset_expr) = &offset {
            let val = ExpressionExecutor::execute_scalar(offset_expr)?;
            Self::cast_delimiter_val(val)
        } else {
            0
        };

        let modifier = if limit.is_none() && offset.is_none() {
            None
        } else {
            Some(BoundResultModifier::BoundLimitModifier(
                BoundLimitModifier::new(limit_value, offsert_value, limit, offset),
            ))
        };

        Ok(modifier)
    }
}
