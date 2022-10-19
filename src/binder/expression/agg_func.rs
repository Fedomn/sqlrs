use std::fmt;

use arrow::datatypes::DataType;
use sqlparser::ast::{Function, FunctionArg, FunctionArgExpr};

use super::BoundExpr;
use crate::binder::{BindError, Binder};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AggFunc {
    Count,
    Sum,
    Min,
    Max,
}

impl fmt::Display for AggFunc {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AggFunc::Count => write!(f, "Count"),
            AggFunc::Sum => write!(f, "Sum"),
            AggFunc::Min => write!(f, "Min"),
            AggFunc::Max => write!(f, "Max"),
        }
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BoundAggFunc {
    pub func: AggFunc,
    pub exprs: Vec<BoundExpr>,
    pub return_type: DataType,
    pub distinct: bool,
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
                exprs: args.clone(),
                return_type: DataType::Int64,
                distinct: func.distinct,
            },
            "sum" => BoundAggFunc {
                func: AggFunc::Sum,
                exprs: args.clone(),
                return_type: args[0].return_type().unwrap(),
                distinct: func.distinct,
            },
            "min" => BoundAggFunc {
                func: AggFunc::Min,
                exprs: args.clone(),
                return_type: args[0].return_type().unwrap(),
                distinct: func.distinct,
            },
            "max" => BoundAggFunc {
                func: AggFunc::Max,
                exprs: args.clone(),
                return_type: args[0].return_type().unwrap(),
                distinct: func.distinct,
            },
            _ => unimplemented!("not implmented agg func {}", func.name),
        };
        Ok(BoundExpr::AggFunc(expr))
    }
}

impl fmt::Debug for BoundAggFunc {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let expr = if self.exprs.len() == 1 {
            format!("{:?}", self.exprs[0])
        } else {
            format!("{:?}", self.exprs)
        };
        let distinct = if self.distinct { "Distinct" } else { "" };
        write!(
            f,
            "{}{}({}):{}",
            distinct, self.func, expr, self.return_type
        )
    }
}
