use arrow::datatypes::DataType;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use super::BoundExpr;
use crate::binder::{BindError, Binder};

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundAggFunc {
    pub func: AggFunc,
    pub expr: Vec<BoundExpr>,
    pub return_type: DataType,
}

impl Binder {
    pub fn bind_agg_func(&mut self, func: &Function) -> Result<BoundExpr, BindError> {
        let mut args = vec![];
        for arg in &func.args {
            let arg = match arg {
                FunctionArg::Named { arg, .. } => arg,
                FunctionArg::Unnamed(arg) => arg,
            };
            match arg {
                FunctionArgExpr::Expr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    args.push(expr);
                }
                FunctionArgExpr::QualifiedWildcard(_) => todo!(),
                FunctionArgExpr::Wildcard => todo!(),
            }
        }

        let expr = match func.name.to_string().to_lowercase().as_str() {
            "count" => BoundAggFunc {
                func: AggFunc::Count,
                expr: args.clone(),
                return_type: DataType::Int64,
            },
            "sum" => BoundAggFunc {
                func: AggFunc::Sum,
                expr: args.clone(),
                return_type: args[0].return_type().unwrap(),
            },
            "min" => BoundAggFunc {
                func: AggFunc::Min,
                expr: args.clone(),
                return_type: args[0].return_type().unwrap(),
            },
            "max" => BoundAggFunc {
                func: AggFunc::Max,
                expr: args.clone(),
                return_type: args[0].return_type().unwrap(),
            },
            _ => unimplemented!("not implmented agg func {}", func.name),
        };
        Ok(BoundExpr::AggFunc(expr))
    }
}
