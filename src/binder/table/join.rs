use std::fmt;

use sqlparser::ast::{JoinConstraint, JoinOperator};

use super::*;
use crate::binder::BoundExpr;

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
    On(BoundExpr),
    None,
}

impl Binder {
    pub fn bind_join_operator(
        &mut self,
        join_operator: &JoinOperator,
    ) -> Result<(JoinType, JoinCondition), BindError> {
        match join_operator {
            JoinOperator::Inner(constraint) => {
                Ok((JoinType::Inner, self.bind_join_constraint(constraint)?))
            }
            JoinOperator::LeftOuter(constraint) => {
                Ok((JoinType::Left, self.bind_join_constraint(constraint)?))
            }
            JoinOperator::RightOuter(constraint) => {
                Ok((JoinType::Right, self.bind_join_constraint(constraint)?))
            }
            JoinOperator::FullOuter(constraint) => {
                Ok((JoinType::Full, self.bind_join_constraint(constraint)?))
            }
            JoinOperator::CrossJoin => Ok((JoinType::Cross, JoinCondition::None)),
            _ => todo!(),
        }
    }

    fn bind_join_constraint(
        &mut self,
        constraint: &JoinConstraint,
    ) -> Result<JoinCondition, BindError> {
        match constraint {
            JoinConstraint::On(expr) => Ok(JoinCondition::On(self.bind_expr(expr)?)),
            _ => unimplemented!("not supported join constraint {:?}", constraint),
        }
    }
}
