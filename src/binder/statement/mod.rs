use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{Query, SelectItem};

use super::expression::BoundExpr;
use super::table::BoundTableRef;
use super::{BindError, Binder};

pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<(), BindError> {
        let select = match &query.body {
            Select(select) => &**select,
            _ => todo!(),
        };

        // currently, only support select one table
        let _from_table = self.bind_table_with_joins(&select.from[0])?;

        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(_expr) => {}
                SelectItem::ExprWithAlias { expr: _, alias: _ } => todo!(),
                SelectItem::QualifiedWildcard(_) => todo!(),
                SelectItem::Wildcard => todo!(),
            }
        }

        Ok(())
    }
}
