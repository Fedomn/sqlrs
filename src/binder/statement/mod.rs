use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{Query, SelectItem};

use super::expression::BoundExpr;
use super::table::BoundTableRef;
use super::{BindError, Binder};

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
}

#[derive(Debug)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        let select = match &query.body {
            Select(select) => &**select,
            _ => todo!(),
        };

        // currently, only support select one table
        let from_table = self.bind_table_with_joins(&select.from[0])?;

        let mut select_list = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr: _, alias: _ } => todo!(),
                SelectItem::QualifiedWildcard(_) => todo!(),
                SelectItem::Wildcard => todo!(),
            }
        }

        Ok(BoundSelect {
            select_list,
            from_table: Some(from_table),
        })
    }
}
