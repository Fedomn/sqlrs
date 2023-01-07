use std::slice;

use derive_new::new;

use super::{
    BindError, Binder, BoundCastExpression, BoundConstantExpression, BoundExpression,
    BoundExpressionBase, ColumnAliasData, SqlparserResolver,
};
use crate::types_v2::{LogicalType, ScalarValue};

#[derive(new)]
pub struct ExpressionBinder<'a> {
    pub(crate) binder: &'a mut Binder,
    #[new(default)]
    pub(crate) column_alias_data: Option<ColumnAliasData>,
}

impl ExpressionBinder<'_> {
    pub fn set_column_alias_data(&mut self, column_alias_data: ColumnAliasData) {
        self.column_alias_data = Some(column_alias_data);
    }

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
            sqlparser::ast::Expr::TypedString { data_type, value } => {
                self.bind_typed_string(data_type, value, result_names, result_types)
            }
            sqlparser::ast::Expr::Interval { .. } => {
                self.bind_interval_expr(expr, result_names, result_types)
            }
            other => Err(BindError::UnsupportedExpr(other.to_string())),
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
            sqlparser::ast::BinaryOperator::And | sqlparser::ast::BinaryOperator::Or => {
                self.bind_conjunction_expression(left, op, right, result_names, result_types)
            }
            other => Err(BindError::UnsupportedExpr(other.to_string())),
        }
    }

    /// TypedString: A constant of form `<data_type> 'value'`.
    fn bind_typed_string(
        &mut self,
        data_type: &sqlparser::ast::DataType,
        value: &str,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        // A constant of form `<data_type> 'value'`.
        let val = sqlparser::ast::Value::SingleQuotedString(value.to_string());
        let constant_expr = self.bind_constant_expr(&val, &mut vec![], &mut vec![])?;
        let target_type = LogicalType::try_from(data_type.clone())?;
        let expr = BoundCastExpression::try_add_cast_to_type(constant_expr, target_type, true)?;
        result_names.push(expr.alias());
        result_types.push(expr.return_type());
        Ok(expr)
    }

    /// bind a interval expression, currently only support one DateTimeFiled, such as: `interval '1'
    /// day`. So if value contains unit, such as `interval '1 year 2 month'`, current binder will
    /// return error. To support this, we need split the value into parts, and parse each part in
    /// loop, so is more complex for now.
    fn bind_interval_expr(
        &mut self,
        expr: &sqlparser::ast::Expr,
        result_names: &mut Vec<String>,
        result_types: &mut Vec<LogicalType>,
    ) -> Result<BoundExpression, BindError> {
        match expr {
            sqlparser::ast::Expr::Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => {
                if leading_precision.is_some()
                    || last_field.is_some()
                    || fractional_seconds_precision.is_some()
                {
                    return Err(BindError::UnsupportedExpr(
                        "Unsupported Interval Expression".to_string(),
                    ));
                }

                let val = SqlparserResolver::resolve_expr_to_string(value)?;
                let num: i64 = val.parse().map_err(|e| {
                    BindError::UnsupportedExpr(format!(
                        "Interval value must be a number, but got {}",
                        e
                    ))
                })?;

                let scalar = match leading_field {
                    Some(v) => {
                        match v {
                            // convert to IntervalYearMonth
                            sqlparser::ast::DateTimeField::Year => {
                                ScalarValue::IntervalYearMonth(Some(num as i32 * 12))
                            }
                            sqlparser::ast::DateTimeField::Month => {
                                ScalarValue::IntervalYearMonth(Some(num as i32))
                            }
                            // convert to IntervalDayTime
                            sqlparser::ast::DateTimeField::Week => {
                                ScalarValue::IntervalDayTime(Some(num * 7 * 24 * 60 * 60 * 1000))
                            }
                            sqlparser::ast::DateTimeField::Day => {
                                ScalarValue::IntervalDayTime(Some(num * 24 * 60 * 60 * 1000))
                            }
                            sqlparser::ast::DateTimeField::Hour => {
                                ScalarValue::IntervalDayTime(Some(num * 60 * 60 * 1000))
                            }
                            sqlparser::ast::DateTimeField::Minute => {
                                ScalarValue::IntervalDayTime(Some(num * 60 * 1000))
                            }
                            sqlparser::ast::DateTimeField::Second => {
                                ScalarValue::IntervalDayTime(Some(num * 1000))
                            }
                            other => {
                                return Err(BindError::UnsupportedExpr(format!(
                                    "Unsupported Interval unit: {:?}",
                                    other
                                )))
                            }
                        }
                    }
                    None => {
                        return Err(BindError::UnsupportedExpr(
                            "Interval must have DataTimeField".to_string(),
                        ))
                    }
                };

                let base =
                    BoundExpressionBase::new(format!("{:?}", scalar), scalar.get_logical_type());
                result_names.push(base.alias.clone());
                result_types.push(base.return_type.clone());
                let expr = BoundExpression::BoundConstantExpression(BoundConstantExpression::new(
                    base, scalar,
                ));
                Ok(expr)
            }
            _ => Err(BindError::UnsupportedExpr(
                "expect interval expr".to_string(),
            )),
        }
    }
}
