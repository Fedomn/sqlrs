use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{Query, SelectItem};

use super::expression::BoundExpr;
use super::table::BoundTableRef;
use super::{BindError, Binder, BoundColumnRef};

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
}

#[derive(Debug)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        let select = match &query.body {
            Select(select) => &**select,
            _ => todo!(),
        };

        // currently, only support select one table
        let from_table = if select.from.is_empty() {
            None
        } else {
            Some(self.bind_table_with_joins(&select.from[0])?)
        };

        // bind select list
        let mut select_list = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let expr = self.bind_expr(expr)?;
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr: _, alias: _ } => todo!(),
                SelectItem::QualifiedWildcard(_) => todo!(),
                SelectItem::Wildcard => {
                    select_list.extend_from_slice(self.bind_all_columns_in_context().as_slice());
                }
            }
        }

        // bind where clause
        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;

        let group_by = select
            .group_by
            .iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;

        Ok(BoundSelect {
            select_list,
            from_table,
            where_clause,
            group_by,
        })
    }

    fn bind_all_columns_in_context(&mut self) -> Vec<BoundExpr> {
        let mut columns = vec![];
        for table_catalog in self.context.tables.values() {
            for column in table_catalog.get_all_columns() {
                let column_ref = BoundExpr::ColumnRef(BoundColumnRef {
                    column_catalog: column,
                });
                columns.push(column_ref);
            }
        }
        columns
    }
}
