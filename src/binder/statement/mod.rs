use sqlparser::ast::SetExpr::Select;
use sqlparser::ast::{Join, JoinOperator, Query, SelectItem, TableWithJoins};

use super::expression::BoundExpr;
use super::table::BoundTableRef;
use super::{BindError, Binder, BoundAlias, BoundColumnRef, EMPTY_DATABASE_ID};

#[derive(Debug)]
pub enum BoundStatement {
    Select(BoundSelect),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundSelect {
    pub select_list: Vec<BoundExpr>,
    pub from_table: Option<BoundTableRef>,
    pub where_clause: Option<BoundExpr>,
    pub group_by: Vec<BoundExpr>,
    pub limit: Option<BoundExpr>,
    pub offset: Option<BoundExpr>,
    pub order_by: Vec<BoundOrderBy>,
    pub select_distinct: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BoundOrderBy {
    pub expr: BoundExpr,
    pub asc: bool,
}

impl Binder {
    pub fn bind_select(&mut self, query: &Query) -> Result<BoundSelect, BindError> {
        let select = match &*query.body {
            Select(select) => &**select,
            _ => todo!(),
        };

        // currently, only support select one table
        let mut from_table = if select.from.is_empty() {
            None
        } else if select.from.len() > 1 {
            // merge select from multiple tables into one cross join
            // TODO: add more checks
            let first_talbe = select.from[0].clone();
            let joins = select.from[1..]
                .iter()
                .map(|a| Join {
                    relation: a.relation.clone(),
                    join_operator: JoinOperator::CrossJoin,
                })
                .collect();
            let table_with_joins = TableWithJoins {
                relation: first_talbe.relation,
                joins,
            };
            Some(self.bind_table_with_joins(&table_with_joins)?)
        } else {
            Some(self.bind_table_with_joins(&select.from[0])?)
        };
        let bound_table_id = from_table
            .clone()
            .map(|t| t.bound_table_id())
            .unwrap_or_else(|| EMPTY_DATABASE_ID.to_string());

        // bind select list
        let mut select_list = vec![];
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let mut expr = self.bind_expr(expr)?;
                    self.rewrite_scalar_subquery(&mut expr, &mut from_table);
                    select_list.push(expr);
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let mut expr = self.bind_expr(expr)?;
                    self.rewrite_scalar_subquery(&mut expr, &mut from_table);
                    self.context.aliases.insert(alias.to_string(), expr.clone());
                    select_list.push(BoundExpr::Alias(BoundAlias {
                        expr: Box::new(expr),
                        column_id: alias.to_string().to_lowercase(),
                        table_id: bound_table_id.clone(),
                    }));
                }
                SelectItem::QualifiedWildcard(object_name) => {
                    let qualifier = format!("{}", object_name);
                    select_list.extend_from_slice(
                        self.bind_qualified_columns_in_context(qualifier).as_slice(),
                    )
                }
                SelectItem::Wildcard => {
                    select_list.extend_from_slice(self.bind_all_columns_in_context().as_slice());
                }
            }
        }
        let select_distinct = select.distinct;

        // bind where clause
        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;

        // bind group by clause
        let group_by = select
            .group_by
            .iter()
            .map(|expr| self.bind_expr(expr))
            .try_collect()?;

        // bind limit offset
        let limit = query
            .limit
            .as_ref()
            .map(|expr| self.bind_expr(expr))
            .transpose()?;
        let offset = query
            .offset
            .as_ref()
            .map(|offset| self.bind_expr(&offset.value))
            .transpose()?;

        // bind order by clause
        let order_by = query
            .order_by
            .iter()
            .map(|expr| {
                let bound_expr = self.bind_expr(&expr.expr)?;
                Ok(BoundOrderBy {
                    expr: bound_expr,
                    asc: expr.asc.unwrap_or(true),
                })
            })
            .try_collect::<Vec<_>>()?;

        Ok(BoundSelect {
            select_list,
            from_table,
            where_clause,
            group_by,
            limit,
            offset,
            order_by,
            select_distinct,
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

    fn bind_qualified_columns_in_context(&mut self, table_name: String) -> Vec<BoundExpr> {
        let mut columns = vec![];
        for table_catalog in self.context.tables.values() {
            for column in table_catalog.get_all_columns() {
                if column.table_id == table_name {
                    let column_ref = BoundExpr::ColumnRef(BoundColumnRef {
                        column_catalog: column,
                    });
                    columns.push(column_ref);
                }
            }
        }
        columns
    }
}
