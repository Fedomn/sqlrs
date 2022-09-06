use std::fmt;

use arrow::datatypes::DataType;
use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint, JoinOperator};

use super::*;
use crate::binder::{BoundBinaryOp, BoundExpr};

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub left: Box<BoundTableRef>,
    pub right: Box<BoundTableRef>,
    pub join_type: JoinType,
    pub join_condition: JoinCondition,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let join_type = match self {
            JoinType::Inner => "Inner",
            JoinType::Left => "Left",
            JoinType::Right => "Right",
            JoinType::Full => "Full",
            JoinType::Cross => "Cross",
        };
        write!(f, "{}", join_type)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinCondition {
    On {
        /// Equijoin clause expressed as pairs of (left, right) join columns
        on: Vec<(BoundExpr, BoundExpr)>,
        /// Filters applied during join (non-equi conditions)
        filter: Option<BoundExpr>,
    },
    None,
}

impl JoinCondition {
    pub fn add_new_filters(self, added_filters: Vec<BoundExpr>) -> JoinCondition {
        match self {
            JoinCondition::On { on, filter } => {
                let new_filter = added_filters
                    .into_iter()
                    .reduce(|a, b| {
                        BoundExpr::BinaryOp(BoundBinaryOp {
                            op: BinaryOperator::And,
                            left: Box::new(a),
                            right: Box::new(b),
                            return_type: Some(DataType::Boolean),
                        })
                    })
                    .map(Some)
                    .unwrap_or(None);

                let new_filter = match (new_filter.clone(), filter.clone()) {
                    (None, None) => None,
                    (None, Some(_)) => filter,
                    (Some(_), None) => new_filter,
                    (Some(a), Some(b)) => Some(BoundExpr::BinaryOp(BoundBinaryOp {
                        op: BinaryOperator::And,
                        left: Box::new(a),
                        right: Box::new(b),
                        return_type: Some(DataType::Boolean),
                    })),
                };
                JoinCondition::On {
                    on,
                    filter: new_filter,
                }
            }
            JoinCondition::None => JoinCondition::None,
        }
    }
}

impl Binder {
    pub fn bind_join_operator(
        &mut self,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
        join_operator: &JoinOperator,
    ) -> Result<(JoinType, JoinCondition), BindError> {
        match join_operator {
            JoinOperator::Inner(constraint) => Ok((
                JoinType::Inner,
                self.bind_join_constraint(left_schema, right_schema, constraint)?,
            )),
            JoinOperator::LeftOuter(constraint) => Ok((
                JoinType::Left,
                self.bind_join_constraint(left_schema, right_schema, constraint)?,
            )),
            JoinOperator::RightOuter(constraint) => Ok((
                JoinType::Right,
                self.bind_join_constraint(left_schema, right_schema, constraint)?,
            )),
            JoinOperator::FullOuter(constraint) => Ok((
                JoinType::Full,
                self.bind_join_constraint(left_schema, right_schema, constraint)?,
            )),
            JoinOperator::CrossJoin => Ok((JoinType::Cross, JoinCondition::None)),
            _ => todo!(),
        }
    }

    fn bind_join_constraint(
        &mut self,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
        constraint: &JoinConstraint,
    ) -> Result<JoinCondition, BindError> {
        match constraint {
            JoinConstraint::On(expr) => {
                // left and right columns that match equi-join pattern
                let mut on_keys: Vec<(BoundExpr, BoundExpr)> = vec![];
                // expression that didn't match equi-join pattern
                let mut filter = vec![];

                self.extract_join_keys(expr, &mut on_keys, &mut filter, left_schema, right_schema)?;

                // combine multiple filter exprs into one BinaryExpr
                let join_filter = filter.into_iter().reduce(|acc, expr| {
                    BoundExpr::BinaryOp(BoundBinaryOp {
                        op: BinaryOperator::And,
                        left: Box::new(acc),
                        right: Box::new(expr),
                        return_type: Some(DataType::Boolean),
                    })
                });
                // TODO: handle cross join if on_keys is empty
                Ok(JoinCondition::On {
                    on: on_keys,
                    filter: join_filter,
                })
            }
            _ => unimplemented!("not supported join constraint {:?}", constraint),
        }
    }

    /// original idea from datafusion planner.rs
    /// Extracts equijoin ON condition be a single Eq or multiple conjunctive Eqs
    /// Filters matching this pattern are added to `accum`
    /// Filters that don't match this pattern are added to `accum_filter`
    /// Examples:
    /// ```text
    /// foo = bar => accum=[(foo, bar)] accum_filter=[]
    /// foo = bar AND bar = baz => accum=[(foo, bar), (bar, baz)] accum_filter=[]
    /// foo = bar AND baz > 1 => accum=[(foo, bar)] accum_filter=[baz > 1]
    /// ```
    fn extract_join_keys(
        &mut self,
        expr: &Expr,
        accum: &mut Vec<(BoundExpr, BoundExpr)>,
        accum_filter: &mut Vec<BoundExpr>,
        left_schema: &TableSchema,
        right_schema: &TableSchema,
    ) -> Result<(), BindError> {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::Eq => {
                    let left = self.bind_expr(left)?;
                    let right = self.bind_expr(right)?;
                    match (&left, &right) {
                        // example: foo = bar
                        (BoundExpr::ColumnRef(l), BoundExpr::ColumnRef(r)) => {
                            // reorder left and right joins keys to pattern: (left, right)
                            if left_schema.contains_key(&l.column_catalog)
                                && right_schema.contains_key(&r.column_catalog)
                            {
                                accum.push((left, right));
                            } else if left_schema.contains_key(&r.column_catalog)
                                && right_schema.contains_key(&l.column_catalog)
                            {
                                accum.push((right, left));
                            } else {
                                accum_filter.push(self.bind_expr(expr)?);
                            }
                        }
                        // example: baz = 1
                        _other => {
                            accum_filter.push(self.bind_expr(expr)?);
                        }
                    }
                }
                BinaryOperator::And => {
                    // example: foo = bar AND baz > 1
                    if let Expr::BinaryOp { left, op: _, right } = expr {
                        self.extract_join_keys(
                            left,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                        self.extract_join_keys(
                            right,
                            accum,
                            accum_filter,
                            left_schema,
                            right_schema,
                        )?;
                    }
                }
                _other => {
                    // example: baz > 1
                    accum_filter.push(self.bind_expr(expr)?);
                }
            },
            _other => {
                // example: baz in (xxx), something else will convert to filter logic
                accum_filter.push(self.bind_expr(expr)?);
            }
        }
        Ok(())
    }
}
